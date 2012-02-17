-- |STINGER.SimplestParallel
--
-- Simplest and slowest implementation for STINGER data structure and analyses combination.
-- Is used for API prototyping.
-- This version is extended with parallel execution of analyses.

{-# LANGUAGE GADTs, TypeFamilies, MultiParamTypeClasses, TypeOperators, FunctionalDependencies #-}
{-# LANGUAGE FlexibleContexts, FlexibleInstances, UndecidableInstances, EmptyDataDecls #-}
{-# LANGUAGE IncoherentInstances, NoMonomorphismRestriction #-}

module STINGER.SimplestParallel(
	  Index	-- from G500.
	-- HList
	, Nil
	, (:.)

	-- representation exported abstractly
	, STINGER
	-- How to create a new STINGER.
	, stingerNew

	-- An analysis monad to create operations with STINGER.
	, STINGERM
	, runAnalysesStack

	-- local values processing.
	, Value
	, Composed
	, localValue
	, cst
	, ($=)
	, (+.), (-.), (*.), divV
	, (===), (=/=)

	-- Analysis type. Abstract.
	, Analysis
	, AnM
	, onEdges
	, anIf
	, getAnalysisResult
	, putAnalysisResult
	, incrementAnalysisResult
	, RequiredAnalyses
	-- how to create basic analysis, one which does not depend on the other.
	, basicAnalysis
	-- derived analysis, dependent on some other.
	, derivedAnalysis
	, EnabledAnalysis, EnabledAnalyses
	) where

import Control.Concurrent
--import Control.Concurrent.Chan
import Control.Concurrent.STM.TChan
import Control.Monad
import Control.Monad.State
import Control.Monad.STM
import Data.Array
import Data.Array.IO
import Data.Bits
import Data.Int
import Data.IORef
import Data.List (intersperse, sort)
import Data.Maybe (fromMaybe, catMaybes)
--import qualified Data.Map as Map
import qualified Data.Set as Set
import Data.Word
import System.IO
import System.Time

import G500.Index
import STINGER.HList

import qualified STINGER.IntSet as ISet
import qualified STINGER.IntMap as IMap


-------------------------------------------------------------------------------
-- A (already not) very simple representation.
-- Allows one to work on graphs in parallel.

analysisSliceSizeShift :: Int
analysisSliceSizeShift = 8

analysisSliceSize :: Int
analysisSliceSize = 2^analysisSliceSizeShift

analysisSliceSizeMask :: Int
analysisSliceSizeMask = analysisSliceSize - 1

type IntMap a = IMap.IntMap a 
type IntSet = ISet.IntSet
type IndexSet = Set.Set Index

type VertexAnalyses = IntMap Int64
type AnalysesMap = IntMap VertexAnalyses

-- First is analysis index, then vertex' local index shifted right by analysisSliceSizeShift
-- and the lat one is vertex' local index modulo analysisSliceSize.
-- This should be relatively small map. Constant number of analyses multiplied by
-- 2^30 (max number of vertices per thread) divided by analysisSliceSize.
-- Expect it to be about 2^12 or less.
type AnalysesArrays = Array Int (Array Int (IOUArray Int Int64))

type EdgeSet = IntMap IndexSet

-- |A representation parametrized by analyses required.
data STINGER msg as = STINGER {
	  stingerMaxNodes		:: !Int
	, stingerNodeIndex		:: !Int
	, stingerBatchCounter		:: !Int
	, stingerEdges			:: !EdgeSet
	-- Results of analyses.
	, stingerAnalyses		:: !AnalysesArrays
	-- Nodes affected in current batch.
	, stingerNodesAffected		:: !IntSet
	-- what analyses were changed in affected nodes.
	, stingerAnalysesAffected	:: !(IntMap Int)
	, stingerChannels		:: !(Array Int (TChan msg))
	-- added per portion.
	, stingerPortionEdges		:: (Array Int EdgeSet)
	-- increments for other nodes.
	-- a map from node index to analysis increments.
	, stingerOthersAnalyses		:: !(IntMap AnalysesMap)
	}

-- |Monad to operate with STINGER.
type STINGERM msg as a = StateT (STINGER msg as) IO a


-------------------------------------------------------------------------------
-- Main STINGER API.

-- |Create a STINGER structure for parallel STINGER processing.
-- Usage: stinger <- stingerNew maxJobNodes thisNodeIndex
stingerNew :: HLength as => Int -> Int -> Array Int (TChan msg) -> IO (STINGER msg as)
stingerNew maxNodes nodeIndex chans = do
	let forwardResult = undefined
	let analysisProjection :: STINGER msg as -> as
	    analysisProjection = error "analysisProjection result should be trated abstractly!"
	let analysisCount = hLength $ analysisProjection forwardResult
	let analysisHighIndex = analysisCount-1
	analysisArrays <- liftM (array (0,analysisHighIndex)) $ forM [0..analysisHighIndex] $ \ai -> do
		aArr <- stingerNewAnalysisSliceArray
		return (ai,array (0,0) [(0,aArr)])
	let result = STINGER maxNodes nodeIndex 0 IMap.empty analysisArrays
			ISet.empty IMap.empty chans (error "no portion edges!") IMap.empty
	return (result `asTypeOf` forwardResult)
	where

stingerNewAnalysisSliceArray :: IO (IOUArray Int Int64)
stingerNewAnalysisSliceArray = newArray (0,analysisSliceSize-1) 0

stingerFillPortionEdges :: [(Index,Index)] -> STINGERM msg as ()
stingerFillPortionEdges edges = do
	nodeIndex <- liftM stingerNodeIndex get
	maxNodes <- liftM stingerMaxNodes get
	let len = length edges
	let sets = scanl (update maxNodes nodeIndex) IMap.empty edges
	let arr = listArray (0,len) sets
	modify $! \st -> st {
		  stingerPortionEdges = arr
		, stingerOthersAnalyses = IMap.empty
		}
	where
		update :: Int -> Int -> EdgeSet -> (Index,Index) -> EdgeSet
		update maxNodes nodeIndex oldSet (fromI, toI)
			| fromNode == nodeIndex && toNode == nodeIndex =
				IMap.insertWith Set.union toIndex (Set.singleton fromI) $
				IMap.insertWith Set.union fromIndex (Set.singleton toI) $
				oldSet 
			| fromNode == nodeIndex =
				IMap.insertWith Set.union fromIndex (Set.singleton toI) $
				oldSet 
			| toNode == nodeIndex =
				IMap.insertWith Set.union toIndex (Set.singleton fromI) $
				oldSet 
			| otherwise = oldSet
			where
				toInt2 (a,b) = (fromIntegral a, fromIntegral b)
				(fromIndex,fromNode) = toInt2 $ fromI `divMod` fromIntegral maxNodes
				(toIndex,toNode) = toInt2 $ toI `divMod` fromIntegral maxNodes

stingerCommitNewPortion :: STINGERM msg as ()
stingerCommitNewPortion = do
	st <- get
	let portionEdges = stingerPortionEdges st
	let (_,highest) = bounds portionEdges
	let latestUpdates = portionEdges ! highest
{-
	liftIO $ putStrLn $ "Latest updates "++show latestUpdates
---}
	put $! st {
		  stingerPortionEdges = error "stingerPortionEdges accessed outside of transaction."
		, stingerEdges = IMap.unionWith Set.union latestUpdates $ stingerEdges st
		}

stingerEdgeExists :: Int -> Index -> Index -> STINGERM msg as Bool
stingerEdgeExists edgeIndex start end
	| start == end = return True
	| otherwise = do
	startIsLocal <- stingerLocalIndex start
	r <- if startIsLocal
		then do
			es <- stingerGetEdgeSet edgeIndex start
			return $ Set.member end es
		else do
			es <- stingerGetEdgeSet edgeIndex end
			return $ Set.member start es
{-
	thisNode <- liftM stingerNodeIndex get
	let text = unlines [
		  "Edge "++show (start,end) ++ "/"++show (end,start)++" "++(if r then "exists" else "doesn't exist")++" at node "++show thisNode++" for edge index "++show edgeIndex
		, "We were looking at "++(if startIsLocal then "start index "++show start else ("end index "++show end))++" edge set."
		]
	liftIO $ putStrLn text
--	liftIO $ hFlush stdout
---}
	return r

stingerGetEdgeSet :: Int -> Index -> STINGERM msg as IndexSet
stingerGetEdgeSet edgeInPortion vertex = do
	index <- stingerIndexToLocal vertex
	st <- get
	let startEdges = IMap.findWithDefault Set.empty index $ stingerEdges st
	let portionEdges = stingerPortionEdges st
	let prevEdges = portionEdges ! edgeInPortion
	let resultEdges = IMap.findWithDefault Set.empty index prevEdges
	return $ Set.union startEdges resultEdges

stingerGetEdges :: Int -> Index -> STINGERM msg as [Index]
stingerGetEdges edgeIndex vertex = do
	liftM Set.toList $ stingerGetEdgeSet edgeIndex vertex

stingerGrowAnalysisArrays :: Int -> STINGERM msg as ()
stingerGrowAnalysisArrays newMaxIndex = do
	analyses <- liftM stingerAnalyses get
	let (lowA, highA) = bounds analyses
	analyses' <- forM [lowA..highA] $ \ai -> do
		let analysisArrays = analyses ! ai
		let (lowAA,highAA) = bounds analysisArrays
		let incr = newMaxIndex - highAA
		addedArrays <- forM [0..incr-1] $ \ii -> do
			slice <- liftIO stingerNewAnalysisSliceArray
			return (highAA+1+ii, slice)
		return (ai,array (lowAA, newMaxIndex) (assocs analysisArrays ++ addedArrays))
	modify $! \st -> st { stingerAnalyses = array (lowA, highA) analyses' }

stingerGetAnalysisArrayIndex :: Int -> Int -> STINGERM msg as (Int,IOUArray Int Int64)
stingerGetAnalysisArrayIndex analysisIndex localIndex = do
	let indexOfSlice = shiftR localIndex analysisSliceSizeShift
	analysisArrays <- liftM ((! analysisIndex) . stingerAnalyses) get
	let (_,highestIndex) = bounds analysisArrays
	if highestIndex < indexOfSlice
		then do
			stingerGrowAnalysisArrays indexOfSlice
			stingerGetAnalysisArrayIndex analysisIndex localIndex
		else do
			let sliceArray = analysisArrays ! indexOfSlice
			return (localIndex .&. analysisSliceSizeMask, sliceArray)

stingerGetAnalysis :: Int -> Index -> STINGERM msg as Int64
stingerGetAnalysis analysisIndex index = do
	error "stingerGetAnalysis does not distinguish between local and not-local indices!!!"
	localIndex <- stingerIndexToLocal index
	(sliceIndex,sliceArray) <- stingerGetAnalysisArrayIndex analysisIndex localIndex
	liftIO $ readArray sliceArray sliceIndex

stingerSetAnalysis :: Int -> Index -> Int64 -> STINGERM msg as ()
stingerSetAnalysis analysisIndex index value = do
	error "stingerSetAnalysis does not distinguish between local and not-local indices!!!"
	localIndex <- stingerIndexToLocal index
	(sliceIndex,sliceArray) <- stingerGetAnalysisArrayIndex analysisIndex localIndex
	liftIO $ writeArray sliceArray sliceIndex value

stingerIncrementAnalysis :: Int -> Index -> Int64 -> STINGERM msg as ()
stingerIncrementAnalysis analysisIndex index 0 = return ()	-- cheap no-op.
stingerIncrementAnalysis analysisIndex index incr = do
	local <- stingerIndexToLocal index
	isLocal <- stingerLocalIndex index
	if isLocal
		then do
{-
			nodeIndex <- liftM stingerNodeIndex get
			liftIO $ putStrLn $ "Incrementing analysis["++show analysisIndex++"]["++show index++"] locally at "++show nodeIndex++" by "++show incr
---}
			(sliceIndex,sliceArray) <- stingerGetAnalysisArrayIndex analysisIndex local
			liftIO $ do
				x <- readArray sliceArray sliceIndex
				writeArray sliceArray sliceIndex (x + incr)
			modify $! \st -> st {
				  stingerNodesAffected = ISet.insert local $ stingerNodesAffected st
				}
		else do
			nodeIndex <- stingerIndexToNodeIndex index
			let joinAnalyses old new = IMap.unionWith (IMap.unionWith (+)) old new
			modify $! \st -> st {
				  stingerOthersAnalyses = IMap.insertWith joinAnalyses nodeIndex
					(IMap.singleton local (IMap.singleton analysisIndex incr)) $
					stingerOthersAnalyses st
				}
{-
			postponed <- liftM stingerOthersAnalyses get
			maxNodes <- liftM stingerMaxNodes get
			let mainMsg = "Postpone increment by "++show incr++" of analysis["++show analysisIndex++"]["++show index++"] to node "++show nodeIndex
			let prettyIncr node localI (ai,incr) =
				"        analysis["++show ai++"]["++show (localI*maxNodes+node)++"] += "++show incr
			let prettyIncrs node (localI,incrs) = map (prettyIncr node localI) $ IMap.toList incrs
			let prettyNodeIncrs node nodeIncrs = ("    node "++show node) :
				concatMap (prettyIncrs node) (IMap.toList nodeIncrs)
			let postponedList = unlines $ concatMap (\(n,incrs) -> prettyNodeIncrs n incrs) $ IMap.toList postponed
			liftIO $ putStrLn $ mainMsg ++"\n"++ postponedList
---}

stingerGetOtherAnalyses :: STINGERM msg as [(Int,AnalysesMap)]
stingerGetOtherAnalyses = do
	liftM (IMap.toList . stingerOthersAnalyses) get

stingerIndexToLocal :: Index -> STINGERM msg as Int
stingerIndexToLocal index = do
	maxNodes <- liftM stingerMaxNodes get
	return $ fromIntegral $ index `div` fromIntegral maxNodes

stingerIndexToNodeIndex :: Index -> STINGERM msg as Int
stingerIndexToNodeIndex index = do
	maxNodes <- liftM stingerMaxNodes get
	return $ fromIntegral $ index `mod` fromIntegral maxNodes

stingerCurrentNodeIndex :: STINGERM msg as Int
stingerCurrentNodeIndex = liftM stingerNodeIndex get

stingerLocalIndex :: Index -> STINGERM msg as Bool
stingerLocalIndex index = do
	nodeIndex <- stingerIndexToNodeIndex index
	liftM (nodeIndex ==) stingerCurrentNodeIndex

-- |Compute "local job" flag from two vertices.
-- Properties:
--   1. local job for (v1,v2) at n1 == not (local job for (v1,v2) at n2)
--      n1 and n2 are node indices for v1 and v2.
--      an exception is for n1 is not our node and n2 is not our node too.
--   2. local job for (v1,v2) == local job for (v2,v1)
-- It is possible for those properties to do not hold for completely
-- local or completely external pair.
stingerLocalJob :: Index -> Index -> STINGERM msg as Bool
stingerLocalJob v1' v2' = do
	n1 <- stingerIndexToNodeIndex v1
	n2 <- stingerIndexToNodeIndex v2
	let randomDir = ((v1 `xor` v2) .&. 1) == 0
	v1Local <- stingerLocalIndex v1
	return $ (randomDir && not v1Local) || (not randomDir && v1Local)
	where
		-- order indices to support property 2.
		v1 = min v1' v2'
		v2 = max v1' v2'

stingerMergeIncrements :: AnalysesMap -> STINGERM msg as ()
stingerMergeIncrements increments = do
{-
	st <- get
	let maxNodes = stingerMaxNodes st
	let node = stingerNodeIndex st
	let header = "Merging increments at node "++show node++":"
	let prettyAIncr index (ai,incr) = concat ["    analysis [",show ai,"][",show index,"] += ",show incr]
	let prettyIndexIncrs (localI,incrs) = map (prettyAIncr (localI*maxNodes+node)) $ IMap.toList incrs
	let incrementsLines = concatMap prettyIndexIncrs $ IMap.toList increments
	let text = unlines $ header : incrementsLines
	liftIO $ putStrLn text
---}
	let flatIncrs = concatMap (\(i,as) -> map (\(ai,incr) -> (i,ai,incr)) $ IMap.toList as) $ IMap.toList increments
	forM_ flatIncrs $ \(localIndex,analysisIndex,incr) -> do
		(sliceIndex,sliceArray) <- stingerGetAnalysisArrayIndex analysisIndex localIndex
		liftIO $ do
			x <- readArray sliceArray sliceIndex
			writeArray sliceArray sliceIndex (x+incr)
	let mergeIncs old new = IMap.unionWith (+) old new
	modify $! \st -> st {
		  stingerNodesAffected = ISet.union (IMap.keysSet increments) $ stingerNodesAffected st
		}

stingerSendToNode :: Int -> msg -> STINGERM msg as ()
stingerSendToNode nodeIndex msg = do
	nodeChan <- liftM ((!nodeIndex) . stingerChannels) get
	liftIO $ atomically $ writeTChan nodeChan msg

stingerSendToNodeOfIndex :: Index -> msg -> STINGERM msg as ()
stingerSendToNodeOfIndex index msg = do
	nodeIndex <- stingerIndexToNodeIndex index
	stingerSendToNode nodeIndex msg 

-- |Get analyses for no more than 100 affected vertices.
stingerGetAffectedAnalyses :: STINGERM msg as [(Index,VertexAnalyses)]
stingerGetAffectedAnalyses = do
	st <- get
	let affected = take 100 $ ISet.toList $ stingerNodesAffected st
	let nodeIndex = fromIntegral $ stingerNodeIndex st
	let maxNodes = fromIntegral $ stingerMaxNodes st
	let toGlobal i = fromIntegral i * maxNodes + nodeIndex
	forM affected $ \localIndex -> do
		analyses <- liftM IMap.unions $ forM (assocs (stingerAnalyses st)) $ \(ai,analysisSlices) -> do
			(sliceIndex, slice) <- stingerGetAnalysisArrayIndex ai localIndex
			x <- liftIO $ readArray slice sliceIndex
			return (IMap.singleton ai x)
		return (toGlobal localIndex, analyses)

stingerClearAffected :: STINGERM msg as ()
stingerClearAffected = modify $! \st -> st { stingerNodesAffected = ISet.empty }

-- |Run the analyses stack.
-- Its' parameters:
--  - function to obtain edges to insert.
--  - a stack of analyses to perform.
runAnalysesStack :: (HLength as, EnabledAnalyses as as) => Int -> IO (Maybe (Array Int Index)) ->
	Analysis as as -> IO ()
runAnalysesStack maxNodes receiveChanges analysesStack
	| analysesParallelizable analysesStack = do
	putStrLn $ "Max nodes "++show maxNodes
	chans <- liftM (listArray (0,maxNodes-1)) $ mapM (const newTChanIO) [0..maxNodes - 1]
	forM_ [0..maxNodes-1] $ \n -> do
		forkIO $ workerThread analysesStack maxNodes n chans
	startTime <- getClockTime
	n <- runLoop chans 0 0
	endTime <- getClockTime
	let timeDiff = diffClockTimes endTime startTime
	let picoSecs = 1000000000000
	let diffPicoSecs = fromIntegral (tdSec timeDiff) * picoSecs + fromIntegral (tdPicosec timeDiff)
	let edgesPerSecond = fromIntegral n * picoSecs / diffPicoSecs
	putStrLn $ "Edges per second "++show edgesPerSecond
	putStrLn $ "Edges "++show n
	putStrLn $ "Seconds "++show (diffPicoSecs / picoSecs)
	return ()
	where
		pairs (x:y:xys) = (x,y) : pairs xys
		pairs _ = []
		runLoop chans pn n = do
			edges <- liftIO receiveChanges
			case edges of
				Nothing -> do
					liftIO $ do
--						putStrLn $ "Stopping threads ("++show pn++")."
						answer <- newChan
						forM_ (elems chans) $ \ch -> do
							atomically (writeTChan ch (Stop answer))
						forM_ (elems chans) $ \_ -> do
							readChan answer
--						putStrLn $ "Done stopping ("++show pn++")."
					return n
				Just edges -> do
					let (low,up) = bounds edges
					let count = div (up-low+1) 2
					answer <- newChan
					-- seed the work.
					forM_ (elems chans) $ \ch -> atomically (writeTChan ch (Portion pn edges answer))
					-- wait for answers.
					putStrLn $ "Waiting for threads ("++show pn++")."
					forM_ [0..maxNodes-1] $ \_ -> 
						readChan answer
					detectSilenceAndDumpState (elems chans)
--					putStrLn $ "Done waiting ("++show pn++")."
					runLoop chans (pn+1) (n+count)
		detectSilence allChans [] = return ()
		detectSilence allChans (chan:chans) = do
			empty <- atomically $ isEmptyTChan chan
			if empty then detectSilence allChans chans
				else detectSilence allChans allChans
		gatherDumpAffected chans = do
			putStrLn $ "Analyses of affected indices:"
			answer <- newChan
			forM_ chans $ \ch -> atomically (writeTChan ch $ GetAffected answer)
			allAffected <- flip (flip foldM []) chans $ \totalAffected _ -> do
				someAffected <- readChan answer
				return $ totalAffected ++someAffected
			let firstSome = take 10 $ sort allAffected
			forM_ firstSome $ \(ix,analyses) -> do
				putStrLn $ "    Index "++show ix
				forM_ (IMap.toList analyses) $ \(ai,a) -> do
					putStrLn $ "        Analysis["++show ai++"]: "++show a
		detectSilenceAndDumpState chans = do
			putStrLn $ "Detecting silence."
			detectSilence chans chans
			putStrLn "All is silent."
			gatherDumpAffected chans
		performInsertionAndAnalyses edgeList = do
			insertAndAnalyzeSimpleSequential analysesStack edgeList

runAnalysesStack maxNodes receiveChanges analysesStack = do
	error "Non-parallelizable analyses aren't supported."

-- |Messages the worker thread can receive.
data Msg as = 
		-- edge changes.
		-- portion number and edges array
		Portion	Int (Array Int Index) (Chan ())
	|	AtomicIncrement	Int AnalysesMap
	|	ContinueIntersection (IntSt as)
	|	GetAffected (Chan [(Index,IntMap Int64)])
	|	Stop (Chan Int)


type MsgChan as = TChan (Msg as)

createMessageChannel :: IO (MsgChan as)
createMessageChannel = newTChanIO

type ChanArr as = Array Int (MsgChan as)
type MsgChanArr as = ChanArr as

workerThread :: (HLength as, EnabledAnalyses as as) => Analysis as as -> Int -> Int -> MsgChanArr as -> IO ()
workerThread analysis maxNodes nodeIndex chans = do
	let ourChan = chans ! nodeIndex
	stinger <- stingerNew maxNodes nodeIndex chans
	let --receiveLoop :: EnabledAnalysis as as => Int -> STINGERM (Msg as) as ()
	    receiveLoop n
		| n <= 0 = return ()
		| otherwise = do
		msg <- liftIO $ atomically $ readTChan ourChan
		case msg of
			AtomicIncrement pn changes -> do
				stingerMergeIncrements changes
				receiveLoop n
			ContinueIntersection env -> do
				interpret env
				receiveLoop (n-1)
			msg -> do
{-
				liftIO $ putStrLn "resending."
--				liftIO $ hFlush stdout
---}
				liftIO $ atomically $ writeTChan ourChan msg
				receiveLoop n
	let --mainLoop :: EnabledAnalysis as as => STINGERM (Msg as) as ()
	    mainLoop = do
--		liftIO $ putStrLn $ "mainloop receiving @"++show nodeIndex
		msg <- liftIO $ atomically $ readTChan ourChan
		case msg of
			Portion pn edges answer -> do
				liftIO $ putStrLn $ "Portion "++show pn++" @"++show nodeIndex
				let es = pairs $ Data.Array.elems edges
				stingerFillPortionEdges es
				stingerClearAffected
				let work n (i,(f,t)) = do
					incr <- workOnEdge analysis i f t
					return $! n + incr
				count <- foldM work 0 $ zip [0..] es
				liftIO $ putStrLn $ "Need to receive "++show count++" msgs @"++show nodeIndex
				receiveLoop count
				sendOtherIncrements pn
				stingerCommitNewPortion
				liftIO $ putStrLn $ "Sending answer."
				liftIO $ writeChan answer ()
				mainLoop
			AtomicIncrement pn changes -> do
				stingerMergeIncrements changes
				mainLoop
			Stop answer -> do
				liftIO $ putStrLn $ "stopped "++show nodeIndex
				liftIO $ writeChan answer nodeIndex
				return ()
			GetAffected answer -> do
				affectedAnalysis <- stingerGetAffectedAnalyses
				liftIO $ writeChan answer affectedAnalysis
				mainLoop
			msg -> do
				liftIO $ atomically $ writeTChan ourChan msg
				mainLoop
	flip runStateT stinger $ mainLoop
	return ()
	where
		pairs (a:b:abs) = (a,b) : pairs abs
		pairs _ = []

sendOtherIncrements :: Int -> STINGERM (Msg as) as ()
sendOtherIncrements pn = do
	increments <- stingerGetOtherAnalyses
	forM_ increments $ \(node,incrs) -> 
		stingerSendToNode node (AtomicIncrement pn incrs)

workOnEdge :: Analysis as as -> Int -> Index -> Index -> STINGERM (Msg as) as Int
workOnEdge analysis edgeIndex fromVertex toVertex = do
	exists <- stingerEdgeExists edgeIndex fromVertex toVertex
	isFromLocal <- stingerLocalIndex fromVertex
	isToLocal <- stingerLocalIndex toVertex
	localStart <- stingerLocalJob fromVertex toVertex
{-
	thisNode <- stingerCurrentNodeIndex
	liftIO $ do
		putStrLn $ unlines [
			  "thisNode "++show thisNode
			, "edgeIndex "++show edgeIndex
			, "fromVertex "++show fromVertex
			, "toVertex "++show toVertex
			, "isFromLocal "++show isFromLocal
			, "isToLocal "++show isToLocal
			, "localStart "++show localStart
			, "exists "++show exists]
--		hFlush stdout
---}
	n <- case (isFromLocal, isToLocal, localStart, exists) of
		-- totally external, shouldn't wait.
		(False, False, _, _) -> return 0
		-- totally internal, wouldn't send or receive.
		(True,True, _, False) -> do
			runStack analysis edgeIndex fromVertex toVertex
			return 0
		-- partially internal and started at our node.
		-- it sends a message and shouldn't wait.
		(_,_,True, False) -> do
			runStack analysis edgeIndex fromVertex toVertex
			return 0
		-- partially internal and started outside.
		-- should wait.
		(_,_,False, False) -> return 1
		(_,_,_, True) -> return 0
	return n

insertAndAnalyzeSimpleSequential :: Analysis as' as -> [(Index, Index)] -> STINGERM msg as ()
insertAndAnalyzeSimpleSequential stack edges =
	error "insertAndAnalyzeSimpleSequential!!!"

runStack :: Analysis as as -> Int -> Index -> Index -> STINGERM (Msg as) as ()
runStack (Analysis action) i start end = do
--	liftIO $ putStrLn $ "statements to interpret:"
--	liftIO $ putStrLn $ show (map snd actionStatements)
	interpret (interpretInitialEnv i actionStatements)
	where
		actionStatements = asStatements $ flip execState (AnSt 0 0 []) $ do
			s <- localValue start
			e <- localValue end
			action (ValueComposed s) (ValueComposed e)

-- |Is analyses stack parallelizable with our method?..
analysesParallelizable :: Analysis as' as -> Bool
analysesParallelizable (Analysis actions) = True

-------------------------------------------------------------------------------
-- Analysis construction monad.

class Storable v where
	toInt64 :: v -> Int64
	fromInt64 :: Int64 -> v

instance Storable Bool where
	toInt64 = fromIntegral . fromEnum
	fromInt64 = toEnum . fromIntegral

instance Storable Int where
	toInt64 = fromIntegral
	fromInt64 = fromIntegral

instance Storable Int64 where
	toInt64 = id
	fromInt64 = id

data AnStatement as where
	-- destination and value
	ASAssign :: (Show a, Storable a) => Value Asgn a -> Value _a a -> AnStatement as
	-- start vertex for edges, end vertex for edges (will be assigned in run-time),
	-- statements to perform.
	ASOnEdges :: Value _a Index -> Value Asgn Index -> AnStatList as -> AnStatement as
	ASAtomicIncr :: Int -> Value _a Index -> Value _b Int64 -> AnStatement as
	ASIf :: Value _a Bool -> AnStatList as -> AnStatList as -> AnStatement as
	ASSetAnalysisResult :: Int -> Value _a Index -> Value _b Int64 -> AnStatement as
	ASFlagVertex :: Value _a Index -> AnStatement as
	ASOnFlaggedVertices :: Value Asgn Index -> AnStatList as -> AnStatement as
	ASContinueEdgeIsect :: IndexSet -> Value Asgn Index -> Value Asgn Index -> Index -> AnStatList as -> AnStatement as

indentShow = indent . show
indent = ("    "++)
indentShowStats stats = map indent $ filter (not . null) $ concatMap (lines . show) stats
instance Show (AnStatement as) where
	show (ASAssign dest what) = show dest ++ " := "++show what
	show (ASOnEdges vertex var stats) = unlines $
		("onEdges "++show vertex++"\\"++show var) : indentShowStats stats
	show (ASAtomicIncr ai index incr) = "analysisResult["++show ai++"]["++show index++"] += "++show incr
	show (ASIf cond thens elses) = unlines $
			("if "++show cond) : "then" : indentShowStats thens ++ ("else" : map indentShow elses)
	show (ASSetAnalysisResult ai index val) = "analysisResult["++show ai++"]["++show index++"] := "++show val
	show (ASFlagVertex index) = "flagVertex "++show index
	show (ASOnFlaggedVertices x ss) = unlines $ ("onFlaggedVertices \\"++show x ++" ->") : indentShowStats ss
	showList xs = \s -> s ++ unlines (map show xs)

type AnStatList as = [AnStatement as]

data AnSt as = AnSt {
	  asValueIndex		:: !Int
	, asStatIndex		:: !Int
	, asStatements		:: !(AnStatList as)
	}

-- this is how we construct analyses.
type AnM as a = State (AnSt as) a

addStatement :: AnStatement as -> AnM as ()
addStatement stat = modify $! \as -> as {
	  asStatIndex = asStatIndex as + 1
	, asStatements = asStatements as ++ [stat]
	}

cutStatements :: AnM as r -> AnM as (AnStatList as, r)
cutStatements act = do
	stats <- liftM asStatements get
	modify $! \as -> as { asStatements = []}
	r <- act
	eStats <- liftM asStatements get
	modify $! \as -> as { asStatements = stats }
	return (eStats, r)

-------------------------------------------------------------------------------
-- Analysis API - enumerating edges.

onEdges :: Value Composed Index -> (Value Composed Index -> AnM as r) -> AnM as r
onEdges vertex act = do
	neighbor <- defineLocal
	(eStats, r) <- cutStatements $ act $ ValueComposed neighbor
	addStatement $ ASOnEdges vertex neighbor eStats
	return r

-------------------------------------------------------------------------------
-- Analysis API - flagging vertices and iterating over them.

flagVertex :: Value any Index -> AnM as ()
flagVertex vertex = do
	addStatement $ ASFlagVertex vertex

onFlaggedVertices :: (Value Composed Index -> AnM as r) -> AnM as r
onFlaggedVertices action = do
	vertex <- defineLocal
	(eStats, r) <- cutStatements $ action $ ValueComposed vertex
	addStatement $ ASOnFlaggedVertices vertex eStats
	return r

-------------------------------------------------------------------------------
-- Analysis API - conditional operator.

anIf :: Value Composed Bool -> AnM as r -> AnM as r -> AnM as r
anIf cond th el = do
	(thStats, r) <- cutStatements th
	(elStats, _) <- cutStatements el
	addStatement $ ASIf cond thStats elStats
	return r

-------------------------------------------------------------------------------
-- Analysis API - keeping analyses' results.

getEnabledAnalyses :: AnM as as
getEnabledAnalyses = return (error "value of getEnabledAnalyses should not be requested.")

getAnalysisIndex :: AnalysisIndex a as => a -> AnM as Int
getAnalysisIndex a = do
	analyses <- getEnabledAnalyses
	return $ analysisIndex a analyses

-- |Fetch analysis result.
getAnalysisResult :: (AnalysisIndex a as) => a -> Value _a Index -> AnM as (Value Composed Int64)
getAnalysisResult analysis vertex = do
	index <- getAnalysisIndex analysis
	return $ ValueComposed $ ValueAnalysisResult index vertex

-- |Store analysis result.
putAnalysisResult :: (AnalysisIndex a as) => a -> Value _a Index -> Value _b Int64 -> AnM as ()
putAnalysisResult analysis vertex value = do
	index <- getAnalysisIndex analysis
	addStatement $ ASSetAnalysisResult index vertex value

-- |Update atomically result with increment.
incrementAnalysisResult :: (AnalysisIndex a as) => a -> Value _a Index -> Value _b Int64 -> AnM as ()
incrementAnalysisResult analysis vertex incr = do
	index <- getAnalysisIndex analysis
	addStatement $ ASAtomicIncr index vertex incr

-------------------------------------------------------------------------------
-- STINGER API - values and expressions.

-- value is assignable.
data Asgn
-- value is composite, thus not assignable.
data Composed

-- |A (modifable) value.
data Value asgn v where
	-- argument's index.
	ValueArgument :: Int -> Value Composed v 
	-- some local variable.
	ValueLocal :: Storable v => Int -> Value Asgn v
	-- constant. we cannot live wothout them.
	ValueConst :: v -> Value Composed v
	-- binary operation.
	ValueBin :: (Show l, Show r) => BinOp l r v -> Value _a l -> Value _b r -> Value Composed v
	-- and unary operations.
	ValueUn :: UnOp a v -> Value _a a -> Value Composed v
	-- cast as composed.
	ValueComposed :: Value _a v -> Value Composed v
	-- address of the analysis result of the value.
	-- index of the analysis in stack, vertex index.
	ValueAnalysisResult :: Int -> Value _b Index -> Value Asgn Int64

data BinOp x y z where
	Plus :: Num x => BinOp x x x
	Minus :: Num x => BinOp x x x
	Mul :: Num x => BinOp x x x
	Div :: Integral x => BinOp x x x
	Equal :: Eq x => BinOp x x Bool

instance Show v => Show (Value asgn v) where
	show v = case v of
		ValueArgument i -> "arg_"++show i
		ValueLocal i -> "var_"++show i
		ValueConst v -> show v
		ValueBin op a b -> unwords ["(",show a,")", show op, "(",show b,")"]
		ValueUn op a -> "unary"
		ValueComposed v -> unwords ["as_composed(",show v,")"]
		ValueAnalysisResult i ix -> "analysis "++show i++" result at "++show ix

instance Show (BinOp x y z) where
	show op = case op of
		Plus -> "+"
		Minus -> "-"
		Mul -> "*"
		Div -> "/"
		Equal -> "=="

data UnOp a r where
	Not :: UnOp Bool Bool
	Negate :: Num v => UnOp v v

-- |Define a (mutable) value local to a computation.
defineLocal :: Storable v => AnM as (Value Asgn v)
defineLocal = do
	modify $! \as -> as { asValueIndex = asValueIndex as + 1 }
	liftM (ValueLocal . asValueIndex) get

-- |Define a local value and assign to it.
localValue :: (Show v, Storable v) => v -> AnM as (Value Asgn v)
localValue def = do
	v <- defineLocal
	v $= cst def
	return v

infixl 6 +., -.
(+.), (-.) :: Num v => Value _a v -> Value _b v -> Value Composed v
a +. b = ValueBin Plus a b
a -. b = ValueBin Minus a b
a *. b = ValueBin Mul a b
divV a b = ValueBin Div a b

(===), (=/=) :: (Show v, Eq v) => Value _a v -> Value _b v -> Value Composed Bool
a === b = ValueBin Equal a b
a =/= b = notV $ a === b

notV = ValueUn Not
negV = ValueUn Negate

cst :: v -> Value Composed v
cst = ValueConst

-- |Assigning a value.
infixr 1 $=
($=) :: (Show v, Storable v) => Value Asgn v -> Value _a v -> AnM as ()
dest $= expr = addStatement $ ASAssign dest expr

-------------------------------------------------------------------------------
-- Interpreting analysis in STINGER monad.

data IntSt as = IntSt {
	  istLocals	:: !(IntMap Int64)
	, isEdgeIndex	:: !Int
	, isConts	:: [AnStatList as]
	}

type AIM as a = StateT (IntSt as) (StateT (STINGER (Msg as) as) IO) a

interpretInitialEnv :: Int -> AnStatList as -> IntSt as
interpretInitialEnv edgeIndex actions =
	IntSt (IMap.empty) edgeIndex [actions]

interpret :: EnabledAnalyses as as => IntSt as -> STINGERM (Msg as) as ()
interpret env = flip evalStateT env $ do
	interpretStatements

interpretStatements :: EnabledAnalyses as as => AIM as ()
interpretStatements = do
	conts <- liftM isConts get
	case conts of
		[] -> return ()
		([]:cs) -> do
			modify $! \st -> st { isConts = cs }
			interpretStatements
		((s:ss):cs) -> do
			modify $! \st -> st { isConts = ss:cs }
			interpretStatement s
			interpretStatements

interpretStatement :: EnabledAnalyses as as => AnStatement as -> AIM as ()
interpretStatement stat = case stat of
	ASAssign dest what -> --liftIO (putStrLn $ show dest ++ " := "++show what) >>
		assignValue dest what
	ASOnEdges startVertex vertexToAssign stats -> do
		interpretOnEdges startVertex vertexToAssign stats
	ASAtomicIncr aIndex vIndex incr -> do
		incr <- interpretValue incr
		vIndex <- interpretValue vIndex
{-
		thisNode <- lift stingerCurrentNodeIndex
		liftIO $ putStrLn $ "thisNode "++show thisNode++", incrementing analysis["++show aIndex++"]["++show vIndex++"] by "++show incr
---}
		lift $ stingerIncrementAnalysis aIndex vIndex incr
	ASIf cond thenStats elseStats -> do
		c <- interpretValue cond
		let stats = if c then thenStats else elseStats
		modify $! \st -> st { isConts = stats : isConts st }
	ASContinueEdgeIsect edgeSet a b thisNodeVertex onEdgeStats -> do
		ei <- liftM isEdgeIndex get
		thisEdgeSet <- lift $ stingerGetEdgeSet ei thisNodeVertex
		let isection = Set.toList $ Set.intersection edgeSet thisEdgeSet
{-
		thisNode <- lift stingerCurrentNodeIndex
		liftIO $ putStrLn $ "thisNode "++show thisNode++", ei "++show ei++", edgeSet "++show edgeSet++", thisEdgeSet "++show thisEdgeSet++", isection "++show isection
---}
		let cont c = ASAssign a (cst c) : ASAssign b (cst c) :
			onEdgeStats
		modify $! \st -> st { isConts = map cont isection ++ isConts st }

assignValue :: Show v => Value Asgn v -> Value _b v -> AIM as ()
assignValue (ValueLocal index) what = do
	--liftIO $ putStrLn $ "assigning "++show index++" with "++show what
	x <- interpretValue what
	modify $! \ist -> ist { istLocals = IMap.insert index (toInt64 x) $ istLocals ist }


interpretOnEdges :: EnabledAnalyses as as => Value _a Index -> Value Asgn Index -> AnStatList as -> AIM as ()

-- special case for edge sets intersection.
interpretOnEdges startVertex1 vertexToAssign1@(ValueLocal i1)
	[ASOnEdges startVertex2 vertexToAssign2@(ValueLocal i2) [ASIf (ValueBin Equal a b) thenStats []]]
	| Just i3 <- uncomposeLocal a
	, Just i4 <- uncomposeLocal b
	, (i1 == i3 && i2 == i4) || (i1 == i4 && i2 == i3) = do
	ei <- liftM isEdgeIndex get
	s1 <- interpretValue startVertex1
	s2 <- interpretValue startVertex2
	l1 <- lift $ stingerLocalIndex s1
	l2 <- lift $ stingerLocalIndex s2
	case (l1,l2) of
		(False, False) -> error "completely non-local computation!"
		(True,True) -> do
			e1 <- lift $ stingerGetEdgeSet ei s1
			e2 <- lift $ stingerGetEdgeSet ei s2
			let cont c = ASAssign vertexToAssign1 (cst c) :
				ASAssign vertexToAssign2 (cst c) :
				thenStats
			let isection = Set.toList $ Set.intersection e1 e2
{-
			thisNode <- lift stingerCurrentNodeIndex
			liftIO $ putStrLn $ "edge "++show (s1,s2)++", isection "++show isection++", e1 "++show e1++", e2 "++show e2
---}
			let conts = map cont isection
			modify $! \st -> st { isConts = conts ++ isConts st }
		-- one is local to us, another is out of our reach.
		(True,False) -> do
			ourEdges <- lift $ stingerGetEdgeSet ei s1
			sendAndStop s2 s1 ourEdges
		(False,True) -> do
			ourEdges <- lift $ stingerGetEdgeSet ei s2
			sendAndStop s1 s2 ourEdges
	where
		uncomposeLocal :: Value _a b -> Maybe Int
		uncomposeLocal (ValueComposed v) = uncomposeLocal v
		uncomposeLocal (ValueLocal i) = Just i
		uncomposeLocal _ = Nothing
		sendAndStop destIndex ourIndex ourEdges = do
			st <- get
			let continueStat = ASContinueEdgeIsect ourEdges vertexToAssign1 vertexToAssign2 destIndex thenStats
			let sendSt = st { isConts = [continueStat] : isConts st }
			lift $ stingerSendToNodeOfIndex destIndex $ ContinueIntersection sendSt
			-- stop interpreting here. It will be continued on another node.
			modify $ \st -> st { isConts = [] }
{-
			thisNode <- lift stingerCurrentNodeIndex
			liftIO $ putStrLn $ "thisNode "++show thisNode++", destIndex "++show destIndex++", ourEdges "++show ourEdges++", ourIndex "++show ourIndex
---}

interpretOnEdges startVertex vertexToAssign stats = do
	error "standalone onEdges is not supported right now!"
{-
	start <- interpretValue startVertex
	edges <- lift $ stingerGetEdges start
	forM_ edges $ \edge -> do
		assignValue vertexToAssign $ cst edge
		interpretStatements stats
-}


interpretValue :: Value _a v -> AIM as v
interpretValue value = case value of
	ValueLocal index -> do
		locals <- liftM istLocals get
		case IMap.lookup index locals of
			Just v -> return (fromInt64 v)
			Nothing -> error $ "local variable #"++show index++" not found in "++show locals++"."
	ValueArgument index -> error "interpreting ValueArgument!!!"
	ValueConst v -> return v
	ValueBin Plus l r -> interpretBin (+) l r
	ValueBin Minus l r -> interpretBin (-) l r
	ValueBin Mul l r -> interpretBin (*) l r
	ValueBin Div l r -> interpretBin (div) l r
	ValueBin Equal l r -> interpretBin (==) l r
	ValueUn Not val -> liftM not $ interpretValue val
	ValueUn Negate val -> liftM negate $ interpretValue val
	ValueComposed v -> interpretValue v
	ValueAnalysisResult analysisIndex vertex -> do
		v <- interpretValue vertex
		lift $ stingerGetAnalysis analysisIndex v
	where
		interpretBin :: (a -> b -> r) -> Value _a a -> Value _b b -> AIM as r
		interpretBin f a b = liftM2 f (interpretValue a) (interpretValue b)


-------------------------------------------------------------------------------
-- STINGER analysis combination.

type family RequiredAnalyses a

data AnalysisNotEnabled a

class EnabledAnalysis a as

class EnabledBool b a as
instance TyCast TRUE b => EnabledBool b  a (a  :. as)
instance (EnabledAnalysis a as) => EnabledBool FALSE a (a' :. as)

instance (EnabledBool b a (a' :. as), TyEq b a a') => EnabledAnalysis a (a' :. as)

class EnabledAnalyses as eas

instance EnabledAnalyses Nil eas
instance (EnabledAnalyses as eas, EnabledAnalysis a eas) => EnabledAnalyses (a :. as) eas

data Analysis as wholeset where
	Analysis :: (EnabledAnalyses (RequiredAnalyses a) as, EnabledAnalyses as wholeset, EnabledAnalyses (a :. as) wholeset) =>
			(Value Composed Index -> Value Composed Index ->
			AnM (a :. as) ()) -> Analysis (a :. as) wholeset

basicAnalysis :: ((RequiredAnalyses a) ~ Nil, EnabledAnalysis a wholeset) =>
	a -> (a -> Value Composed Index -> Value Composed Index -> AnM (a :. Nil) ()) -> Analysis (a :. Nil) wholeset
basicAnalysis analysis edgeInsert = Analysis (edgeInsert analysis)

derivedAnalysis :: (EnabledAnalyses (RequiredAnalyses a) as, EnabledAnalyses as wholeset, EnabledAnalyses (a :. as) wholeset)  =>
	Analysis as wholeset -> a -> (a -> Value Composed Index -> Value Composed Index -> AnM (a :. as) ()) -> Analysis (a :. as) wholeset
derivedAnalysis (Analysis requiredActions) analysis edgeInsert = Analysis combinedActions
	where
		liftAnalyses :: AnM req () -> AnM (a :. req) ()
		liftAnalyses act = do
			s <- get
			let s' = execState act $ s { asStatements = [] }
			put $ s { asStatements = asStatements s ++ (map liftStatement $ asStatements s') }
		liftStatement :: AnStatement as -> AnStatement (a :. as)
		liftStatement stat = case stat of
			ASAssign v e -> ASAssign v e
			ASOnEdges i arg as -> ASOnEdges i arg (map liftStatement as)
			ASAtomicIncr ai vi incr -> ASAtomicIncr ai vi incr
			ASIf cond thens elses -> ASIf cond (map liftStatement thens) (map liftStatement elses)
			ASSetAnalysisResult ai vi val -> ASSetAnalysisResult ai vi val
			ASFlagVertex v -> ASFlagVertex v
			ASOnFlaggedVertices arg stats -> ASOnFlaggedVertices arg $ map liftStatement stats

		combinedActions start end = do
			liftAnalyses $ requiredActions start end
			edgeInsert analysis start end
			


class EnabledAnalysis a as => AnalysisIndex a as where
	analysisIndex :: a -> as -> Int

class AnalysisIndexBool b a as | a as -> b where
	analysisAtHead :: a -> as -> b
	analysisIndexBool :: b -> a -> as -> Int
instance (HLength as, TyCast TRUE b) => AnalysisIndexBool b  a (a  :. as) where
	analysisAtHead _ _ = undefined
	analysisIndexBool _ _ list = hLength $ hTail list
instance (AnalysisIndex a as) => AnalysisIndexBool FALSE a (a' :. as) where
	analysisAtHead _ _ = undefined
	analysisIndexBool _ a list = analysisIndex a $ hTail list

instance (EnabledBool b a (a' :. as), AnalysisIndexBool b a (a' :. as), TyEq b a a') => AnalysisIndex a (a' :. as) where
	analysisIndex a as = analysisIndexBool (analysisAtHead a as) a as

