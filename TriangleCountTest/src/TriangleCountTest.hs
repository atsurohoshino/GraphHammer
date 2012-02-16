-- |TriangleCountTest
--
-- Testing STINGER analyses.
--

{-# LANGUAGE PatternGuards #-}

module Main(main) where

import Control.Monad
import Control.Monad.State
import Data.Array
import Data.Array.IO
import Data.Bits
import Data.IORef
import Data.List
import Data.Word
import System.Environment
import System.Exit
import System.IO

import qualified G500 as G500

import STINGER
import STINGER.TriangleCount

import RectangleGraphGenerator
import OneToOneGraphGenerator

-------------------------------------------------------------------------------
-- Configuration parameters.

-------------------------------------------------------------------------------
-- Generating test data.

data GraphType = G500 String | Rectangle Int Int | OneToOne Int
	deriving (Eq, Show)

testDataReading :: GraphType -> Int -> IO (IO (Maybe (Array Int Index)))
testDataReading gtype batchSize = case gtype of
	G500 fname -> do
		h <- openBinaryFile fname ReadMode
		-- two vertices per edge, 8 bytes per Int64. !!! HACK !!!
		buf <- newArray (0,bytesCount-1) 0
		return $ graph500Reader h buf
	Rectangle w h -> simpleGraphGen ("rectangle regular graph "++show w++"x"++show h) $ regularRectangleGraph h w
	OneToOne count -> simpleGraphGen ("one-to-one graph, "++show count++"-gon") $ oneToOneGraph True count
	where
		vertexSize = 8
		edgeSize = 2*vertexSize
		bytesCount = edgeSize*batchSize
		simpleGraphGen msg edges = do
			putStrLn msg
			let edgeArrays = edgesToArrays edges
			--mapM_ print edgeArrays
			ref <- newIORef edgeArrays
			return $ gen ref
		edgesToArrays [] = []
		edgesToArrays xs = frontEdgesArray : edgesToArrays rest
			where
				(front, rest) = splitAt batchSize xs
				frontEdgeNodes = unpair front
				nFrontNodes = length frontEdgeNodes
				unpair [] = []
				unpair ((a,b) : abs) = a : b : unpair abs
				frontEdgesArray =
					listArray (0,nFrontNodes-1) frontEdgeNodes
		gen ref = do
			xs <- readIORef ref
			case xs of
				[] -> return Nothing
				(a:as) -> do
					writeIORef ref as
					return $ Just a
		graph500Reader h buf = do
			eof <- hIsEOF h
			if eof then return Nothing
				else readBatchPortion h buf
		readVertexIndex :: Array Int Word8 -> Int -> Index
		readVertexIndex a i = foldr (\x i -> i*256 + fromIntegral x) 0 $
			map (a!) [i*vertexSize..(i+1)*vertexSize-1]
		readBatchPortion :: Handle -> IOUArray Int Word8 -> IO (Maybe (Array Int Index))
		readBatchPortion h buf = do
			n <- hGetArray h buf bytesCount
			bytes <- freeze buf
			let edgesCount = n `div` edgeSize
			let indices = map (readVertexIndex bytes) [0..edgesCount*2-1]
			let pairs = listArray (0, edgesCount*2-1) indices
			return $ Just pairs

-------------------------------------------------------------------------------
-- Generate and write a Graph500 data in a format that testDataReading understands.

generateWriteGraph500Data :: String -> Int -> Int -> IO ()
generateWriteGraph500Data fname scale edgeFactor = do
	when (edgeFactor /= 16) $ putStrLn "It is preferable for edgeFactor to be 16."
	(start,end) <- G500.generate scale edgeFactor
	buffer <- mkBuffer
	h <- openBinaryFile fname WriteMode
	write h 0 buffer start end
	hClose h
	where
		verticesExpected :: Index
		verticesExpected = shiftL 1 scale
		totalEdgesExpected :: Index
		totalEdgesExpected = verticesExpected * fromIntegral edgeFactor
		bufferPairs = min verticesExpected 8192
		bufferIndices = bufferPairs*2
		bufferBytes = fromIntegral (8*bufferIndices)

		mkBuffer :: IO (IOUArray Int Word8)
		mkBuffer = newArray (0,bufferBytes-1) 0

		write h start buffer edgeStart edgeEnd = do
			(_,top) <- getBounds edgeStart
			if start > top then return ()
				else do
					fill start buffer edgeStart edgeEnd
					-- !!! HACK !!!
					-- Int64 is not a Storable instance.
					hPutArray h buffer (8*fromIntegral bufferIndices)
					write h (start+bufferPairs) buffer edgeStart edgeEnd
		fill start buffer edgeStart edgeEnd = do
			forM_ [0..bufferPairs-1] $ \i -> do
				s <- readArray edgeStart (start + i)
				e <- readArray edgeEnd (start + i)
				writeIndexAsBytes buffer (fromIntegral i*2  ) s
				writeIndexAsBytes buffer (fromIntegral i*2+1) e
		writeIndexAsBytes :: IOUArray Int Word8 -> Int -> Index -> IO ()
		writeIndexAsBytes arr i ix = do
			forM_ [0..7] $ \s -> do
				writeArray arr (i*8 + s) (fromIntegral $ shiftR ix (s*8))
			

-------------------------------------------------------------------------------
-- Reading the arguments.

data STINGERArgs = SArgs {
	  saBatchSize		:: Int
	, saNodes		:: Int
	, saProcesses		:: Int
	}

readArguments :: IO (Either (String, Int, Int) (GraphType, STINGERArgs))
readArguments = do
	args <- getArgs
	putStrLn $ "Arguments: "++show args
	case runStateT parseArgs args of
		Nothing -> do
			putStrLn $ unlines [
				  "usage: TriangleCountTest -gen-graph500 -fname=<filename> -scale=<scale> [-edgeFactor=<edge factor>]"
				, "    or TriangleCountTest -graph500 -fname=<filename> [stinger arguments]"
				, "    or TriangleCountTest -rectangle -width=<width> [-height=<height>] [stinger arguments]"
				, "    or TriangleCountTest -one-to-one -count=<vertex count> [stinger arguments]"
				, "STINGER arguments are:"
				, "    -batch=<batch size>, default 1000"
				, "    -nodes=<nodes count>, default 1"
				, "    -processes=<processes per node>, default 1"
				, "As STINGER arguments all have defaults, they can be omitted."
				, "Default values will make STINGER work sequentially."
				]
			exitFailure
		Just (result,_) -> return result
	where

matchOpt :: String -> StateT [String] Maybe (Maybe String)
matchOpt opt = do
	(('-':a):as) <- get
	put as
	mplus (do
			"" <- lift $ stripPrefix opt a
			return Nothing)
		(liftM Just (lift $ stripPrefix (opt++"=") a))
intOpt opt = do
	Just v <- matchOpt opt
	case reads v of
		[(i,"")] -> return i
		_ -> mzero
intDefOpt def opt = intOpt opt `mplus` return def
g500 = do
	matchOpt "graph500"
	liftM G500 filenameOpt
rectangle = do
	matchOpt "rectangle"
	w <- intOpt "width"
	h <- intDefOpt w "height"
	return $ Rectangle w h

oneToOne = do
	matchOpt "one-to-one"
	liftM OneToOne $ intOpt "count"

speedMeasurementArgs = do
	gtype <- g500 `mplus` rectangle `mplus` oneToOne
	sa <- stingerArgs
	[] <- get
	return (gtype, sa)

filenameOpt = do
	Just fn <- matchOpt "fname"
	return fn

g500GenArgs = do
	matchOpt "gen-graph500"
	fn <- filenameOpt
	scale <- intOpt "scale"
	edgeFactor <- intDefOpt 16 "edgeFactor"
	return (fn, scale, edgeFactor)

parseArgs = liftM Right speedMeasurementArgs `mplus` liftM Left g500GenArgs

stingerArgs = do
	parseOpts $ SArgs 1000 1 1
	where
		parseOpts sa = do
				sa' <- batch sa `mplus` nodes sa `mplus` processes sa
				parseOpts sa'
			`mplus` return sa
		batch sa = do
			b <- intOpt "batch"
			return $ sa { saBatchSize = b }
		nodes sa = do
			n <- intOpt "nodes"
			return $ sa { saNodes = n }
		processes sa = do
			p <- intOpt "processes"
			return $ sa { saProcesses = p }

-------------------------------------------------------------------------------
-- Running the test.

main = do
	operation <- readArguments
	case operation of
		Right (graphType, stingerArgs) -> do
			gen <- testDataReading graphType (saBatchSize stingerArgs)
			let maxNodes = saNodes stingerArgs
			runAnalysesStack maxNodes gen triangleCount
		Left (fn, scale, edgeFactor) -> do
			generateWriteGraph500Data fn scale edgeFactor

