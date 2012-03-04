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
import qualified G500.Read as GR

import STINGER
import STINGER.TriangleCount

import RectangleGraphGenerator
import OneToOneGraphGenerator

-------------------------------------------------------------------------------
-- Configuration parameters.

-------------------------------------------------------------------------------
-- Generating test data.

testDataReading :: String -> Int -> IO (IO (Maybe (Array Int Index)))
testDataReading fname batchSize = do
	h <- openBinaryFile fname ReadMode
	GR.mkGraph500Reader h batchSize

-------------------------------------------------------------------------------
-- Reading the arguments.

data STINGERArgs = SArgs {
	  saBatchSize		:: Int
	, saNodes		:: Int
	, saProcesses		:: Int
	}

readArguments :: IO (String, STINGERArgs)
readArguments = do
	args <- getArgs
	putStrLn $ "Arguments: "++show args
	case runStateT parseArgs args of
		Nothing -> do
			putStrLn $ unlines [
				  "usage: TriangleCountTest -fname=<filename> [stinger arguments]"
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

speedMeasurementArgs = do
	sa <- stingerArgs
	[] <- get
	return sa

filenameOpt = do
	Just fn <- matchOpt "fname"
	return fn

parseArgs = do
	liftM2 (,) filenameOpt speedMeasurementArgs

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
	(fn, stingerArgs) <- readArguments
	gen <- testDataReading fn (saBatchSize stingerArgs)
	let maxNodes = saNodes stingerArgs
	runAnalysesStack maxNodes gen triangleCount

