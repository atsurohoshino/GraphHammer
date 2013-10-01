{-# LANGUAGE GADTs, TypeFamilies, MultiParamTypeClasses, TypeOperators, FunctionalDependencies #-}
{-# LANGUAGE FlexibleContexts, FlexibleInstances, UndecidableInstances, EmptyDataDecls #-}
{-# LANGUAGE IncoherentInstances, NoMonomorphismRestriction, PatternGuards, BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- |
-- Module    : GraphHammer.SimplestParallel
-- Copyright : (C) 2013 Parallel Scientific Labs, LLC.
-- License   : GPLv2
--
--
-- Simplest and slowest implementation for GraphHammer data structure and analyses combination.
-- Is used for API prototyping.
-- This version is extended with parallel execution of analyses.
module GraphHammer.SimplestParallel(
	  Index	-- from G500.
	-- * HList
	, Nil
	, (:.)

	-- * Representation exported abstractly
	, GraphHammer
	-- ** How to create a new GraphHammer.
	, graphHammerNew

	-- ** An analysis monad to create operations with GraphHammer.
	, GraphHammerM
	, runAnalysesStack

	-- ** Local values processing.
	, Value
	, Composed
	, localValue
	, cst
	, ($=)
	, (+.), (-.), (*.), divV
	, (===), (=/=)

	-- * Analysis type. Abstract.
	, Analysis
	, AnM
	, onEdges
	, anIf
	, getAnalysisResult
	, putAnalysisResult
	, incrementAnalysisResult
	, RequiredAnalyses
	-- ** How to create basic analysis, one which does not depend on the other.
	, basicAnalysis
	-- ** Derived analysis, dependent on some other.
	, derivedAnalysis
	, EnabledAnalysis, EnabledAnalyses
	) where

import Control.Concurrent
import Control.Monad
import Control.Monad.State
import Data.Array
import Data.Array.IO
import qualified Data.Array.Unboxed as UA
import Data.Bits
import Data.Int
import Data.List (sort)
import System.IO
import System.Mem (performGC)
import System.Time

import G500.Index
import GraphHammer.HList

import qualified GraphHammer.IntSet as ISet
import qualified GraphHammer.IntMap as IMap

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

-- type IndexSet = Set.Set Index

type VertexAnalyses = IntMap Int64
type AnalysesMap = IntMap VertexAnalyses
type OtherAnalyses = IntMap AnalysesMap

-- First is analysis index, then vertex' local index shifted right by analysisSliceSizeShift
-- and the lat one is vertex' local index modulo analysisSliceSize.
-- This should be relatively small map. Constant number of analyses multiplied by
-- 2^30 (max number of vertices per thread) divided by analysisSliceSize.
-- Expect it to be about 2^12 or less.
type AnalysesArrays = Array Int32 (Array Int32 (IOUArray Int32 Int64))

type EdgeSet = IntMap VertexSet

-- |A representation parametrized by analyses required.
data GraphHammer as = GraphHammer {
	  graphHammerMaxNodes		:: !Int
	, graphHammerNodeIndex		:: !Int
	, _graphHammerBatchCounter	:: !Int
	, graphHammerEdges			:: !EdgeSet
	-- Results of analyses.
	, graphHammerAnalyses		:: !AnalysesArrays
	-- Nodes affected in current batch.
	, graphHammerNodesAffected	:: !IntSet
	-- what analyses were changed in affected nodes.
	, _graphHammerAnalysesAffected	:: !(IntMap Int)
	, graphHammerChannels	    	:: !(Array Int32 (Chan (Msg as)))
	, graphHammerSendReceiveChannel	:: !(Chan SendReceive)
	-- added per portion.
	, graphHammerPortionEdges		:: (Array Int EdgeSet)
	-- increments for other nodes.
	-- a map from node index to analysis increments.
	, graphHammerOthersAnalyses		:: !OtherAnalyses
	-- a map from node index to continuations.
	, graphHammerContinuationGroups	:: !(IntMap [IntSt as])
	}

-- |Monad to operate with GraphHammer.
type GraphHammerM as a = StateT (GraphHammer as) IO a


-------------------------------------------------------------------------------
-- Working with vertex sets.

data Vertex = Vertex { vertexNode, vertexIndex :: !Int32 }
	deriving (Eq, Ord, Show)

-- vertex set is a map from node to a set of local indices.
type VertexSet = IntMap IntSet

vertexSetEmpty :: forall a. IMap.IntMap a
vertexSetEmpty = IMap.empty

vertexSetIntersection :: IMap.IntMap ISet.IntSet
                      -> IMap.IntMap ISet.IntSet -> IMap.IntMap ISet.IntSet
vertexSetIntersection a b = IMap.filter (not . ISet.null) $
	IMap.intersectionWith (ISet.intersection) a b

vertexSetUnion :: IMap.IntMap ISet.IntSet
                        -> IMap.IntMap ISet.IntSet -> IMap.IntMap ISet.IntSet
vertexSetUnion a b = IMap.unionWith (ISet.union) a b

vertexSetToList :: VertexSet -> [Vertex]
vertexSetToList set = concatMap (\(n,iset) -> map (Vertex n) $ ISet.toList iset) $
	IMap.toList set

vertexToIndex :: (Integral b, Num a) => b -> Vertex -> a
vertexToIndex maxNodes vertex = fromIntegral (vertexNode vertex) +
	fromIntegral maxNodes * fromIntegral (vertexIndex vertex)

indexToVertex :: (Integral b, Integral a) => a -> b -> Vertex
indexToVertex maxNodes idx = Vertex (fromIntegral nodeIndex) (fromIntegral localIndex)
  where
    (localIndex,nodeIndex) = divMod idx (fromIntegral maxNodes)

vertexSetMember :: Vertex -> VertexSet -> Bool
vertexSetMember vertex set' = case IMap.lookup (vertexNode vertex) set' of
	Just set'' -> ISet.member (vertexIndex vertex) set''
	Nothing -> False

{-
vertexSetInsert :: Vertex -> VertexSet -> VertexSet
vertexSetInsert (Vertex node idx) vset = IMap.insertWith ISet.union node (ISet.singleton idx) vset
-}

vertexSetSingleton :: Vertex -> VertexSet
vertexSetSingleton (Vertex node idx) = IMap.singleton node (ISet.singleton idx)

vertexSetSize :: VertexSet -> Int
vertexSetSize set' = IMap.fold (+) 0 $ IMap.map ISet.size set'

-------------------------------------------------------------------------------
-- Main GraphHammer API.

-- |Create a GraphHammer structure for parallel GraphHammer processing.
-- Usage: 
-- @@
-- gh <- graphHammerNew maxJobNodes thisNodeIndex
-- @@
graphHammerNew :: HLength as 
               => Int -- ^ max job nodes
               -> Int -- ^ Node index
               -> Chan SendReceive 
               -> Array Int32 (Chan (Msg as)) 
               -> IO (GraphHammer as)
graphHammerNew maxNodes nodeIndex countingChan chans = do
	dummyAnalysisArrays <- liftM (array (0,0)) $ forM [0..0] $ \ai -> do
		aArr <- graphHammerNewAnalysisSliceArray
		return (ai,array (0,0) [(0,aArr)])
	let forwardResult = GraphHammer maxNodes nodeIndex 0 IMap.empty dummyAnalysisArrays
			ISet.empty IMap.empty chans countingChan (error "no portion edges!") IMap.empty IMap.empty
	let analysisProjection :: HLength as' => GraphHammer as' -> as'
	    analysisProjection = error "analysisProjection result should be trated abstractly!"
	let analysisCount = hLength (analysisProjection forwardResult)
	let analysisHighIndex = analysisCount-1
	analysisArrays <- liftM (array (0,fromIntegral analysisHighIndex)) $ forM [0..fromIntegral analysisHighIndex] $ \ai -> do
		aArr <- graphHammerNewAnalysisSliceArray
		return (ai,array (0,0) [(0,aArr)])
	let result = forwardResult { graphHammerAnalyses = analysisArrays }
	return result

graphHammerCountReceived :: GraphHammerM as ()
graphHammerCountReceived = do
	countChan <- liftM graphHammerSendReceiveChannel get
	liftIO $ writeChan countChan $ Received 1

graphHammerCountSent :: Int -> GraphHammerM as ()
graphHammerCountSent count = do
	nodeIndex <- liftM graphHammerNodeIndex get
	countChan <- liftM graphHammerSendReceiveChannel get
	liftIO $ writeChan countChan $ Sent nodeIndex count

graphHammerNewAnalysisSliceArray :: IO (IOUArray Int32 Int64)
graphHammerNewAnalysisSliceArray = newArray (0,fromIntegral analysisSliceSize-1) 0

graphHammerFillPortionEdges :: [(Index,Index)] -> GraphHammerM as ()
graphHammerFillPortionEdges edges = do
	nodeIndex <- liftM (fromIntegral . graphHammerNodeIndex) get
	maxNodes <- liftM (fromIntegral . graphHammerMaxNodes) get
	let len = length edges
	let sets = scanl (update maxNodes nodeIndex) IMap.empty edges
	let arr = listArray (0,len) sets
	modify $! \st -> last sets `seq` st {
		  graphHammerPortionEdges = arr
		, graphHammerOthersAnalyses = IMap.empty
		}
	where
		update :: Int32 -> Int32 -> EdgeSet -> (Index,Index) -> EdgeSet
		update maxNodes nodeIndex oldSet (fromI, toI)
			| vertexNode fromV == nodeIndex && vertexNode toV == nodeIndex =
				IMap.insertWith vertexSetUnion (vertexIndex fromV) (vertexSetSingleton toV) $!
				IMap.insertWith vertexSetUnion (vertexIndex toV) (vertexSetSingleton fromV) $!
				oldSet 
			| vertexNode fromV == nodeIndex  =
				IMap.insertWith vertexSetUnion (vertexIndex fromV) (vertexSetSingleton toV) $! oldSet
			| vertexNode toV == nodeIndex =
				IMap.insertWith vertexSetUnion (vertexIndex toV) (vertexSetSingleton fromV) $! oldSet
			| otherwise = oldSet
			where
				fromV = indexToVertex maxNodes fromI
				toV = indexToVertex maxNodes toI

graphHammerCommitNewPortion :: GraphHammerM as ()
graphHammerCommitNewPortion = do
    st <- get
    let portionEdges = graphHammerPortionEdges st
        (_,highest) = bounds portionEdges
        latestUpdates = portionEdges ! highest

    put $! st {
              graphHammerPortionEdges = error "graphHammerPortionEdges accessed outside of transaction."
            , graphHammerEdges = IMap.unionWith vertexSetUnion latestUpdates $ graphHammerEdges st
            , graphHammerContinuationGroups = IMap.empty
            }

graphHammerSplitIndex :: Index -> GraphHammerM as Vertex
graphHammerSplitIndex idx = do
	maxNodes <- liftM graphHammerMaxNodes get
	return $ indexToVertex maxNodes idx

graphHammerVertexSetIntersection :: VertexSet -> VertexSet -> GraphHammerM as VertexSet
graphHammerVertexSetIntersection s1 s2 = return $ vertexSetIntersection s1 s2

graphHammerVertexSetIntersectionAsIndices :: VertexSet -> VertexSet -> GraphHammerM as [Index]
graphHammerVertexSetIntersectionAsIndices s1 s2 = do
	maxNodes <- liftM graphHammerMaxNodes get
	let isection = vertexSetIntersection s1 s2
	return $ map (vertexToIndex maxNodes) $ vertexSetToList isection

graphHammerEdgeExists :: Int -> Vertex -> Vertex -> GraphHammerM as Bool
graphHammerEdgeExists edgeIndex start end
	| start == end = return True
	| otherwise = do
	startIsLocal <- graphHammerLocalVertex start
	r <- if startIsLocal
		then do
			es <- graphHammerGetEdgeSet edgeIndex start
			return $ vertexSetMember end es
		else do
			es <- graphHammerGetEdgeSet edgeIndex end
			return $ vertexSetMember start es
	return r

graphHammerGetEdgeSet :: Int -> Vertex -> GraphHammerM as VertexSet
graphHammerGetEdgeSet edgeInPortion vertex = do
	let localIndex = vertexIndex vertex
	st <- get
	let startEdges = IMap.findWithDefault IMap.empty localIndex $ graphHammerEdges st
	let portionEdges = graphHammerPortionEdges st
	let prevEdges = portionEdges ! edgeInPortion
	let resultEdges = IMap.findWithDefault vertexSetEmpty localIndex prevEdges
	return $ IMap.unionWith ISet.union startEdges resultEdges

graphHammerGrowAnalysisArrays :: Int32 -> GraphHammerM as ()
graphHammerGrowAnalysisArrays newMaxIndex = do
	analyses <- liftM graphHammerAnalyses get
	let (lowA, highA) = bounds analyses
	analyses' <- forM [lowA..highA] $ \ai -> do
		let analysisArrays = analyses ! ai
		let (lowAA,highAA) = bounds analysisArrays
		let incr = newMaxIndex - highAA
		addedArrays <- forM [0..incr-1] $ \ii -> do
			slice <- liftIO graphHammerNewAnalysisSliceArray
			return (highAA+1+ii, slice)
		return (ai,array (lowAA, newMaxIndex) (assocs analysisArrays ++ addedArrays))
	modify $! \st -> st { graphHammerAnalyses = array (lowA, highA) analyses' }

graphHammerGetAnalysisArrayIndex :: Int32 -> Int32 -> GraphHammerM as (Int32,IOUArray Int32 Int64)
graphHammerGetAnalysisArrayIndex analysisIndex' localIndex = do
	let indexOfSlice = shiftR localIndex analysisSliceSizeShift
	analysisArrays <- liftM ((! analysisIndex') . graphHammerAnalyses) get
	let (_,highestIndex) = bounds analysisArrays
	if highestIndex < indexOfSlice
		then do
			graphHammerGrowAnalysisArrays (fromIntegral indexOfSlice)
			graphHammerGetAnalysisArrayIndex analysisIndex' localIndex
		else do
			let sliceArray = analysisArrays ! indexOfSlice
			return (localIndex .&. fromIntegral analysisSliceSizeMask, sliceArray)

graphHammerGetAnalysis :: Int -> Index -> GraphHammerM as Int64
graphHammerGetAnalysis analysisIndex index = do
	error "graphHammerGetAnalysis does not distinguish between local and not-local indices!!!"
	localIndex <- graphHammerIndexToLocal index
	(sliceIndex,sliceArray) <- graphHammerGetAnalysisArrayIndex (fromIntegral analysisIndex) localIndex
	liftIO $ readArray sliceArray sliceIndex

graphHammerSetAnalysis :: Int -> Index -> Int64 -> GraphHammerM as ()
graphHammerSetAnalysis analysisIndex index value = do
	error "graphHammerSetAnalysis does not distinguish between local and not-local indices!!!"
	localIndex <- graphHammerIndexToLocal index
	(sliceIndex,sliceArray) <- graphHammerGetAnalysisArrayIndex (fromIntegral analysisIndex) localIndex
	liftIO $ writeArray sliceArray sliceIndex value

graphHammerIncrementAnalysis :: Int -> Index -> Int64 -> GraphHammerM as ()
graphHammerIncrementAnalysis _analysisIndex _index 0 = return ()	-- cheap no-op.
graphHammerIncrementAnalysis analysisIndex index incr = do
	local <- graphHammerIndexToLocal index
	isLocal <- graphHammerLocalIndex index
	if isLocal
		then do
			(sliceIndex,sliceArray) <- graphHammerGetAnalysisArrayIndex (fromIntegral analysisIndex) local
			liftIO $ do
				x <- readArray sliceArray sliceIndex
				writeArray sliceArray sliceIndex (x + incr)
			modify $! \st -> st {
				  graphHammerNodesAffected = ISet.insert local $ graphHammerNodesAffected st
				}
		else do
			nodeIndex <- graphHammerIndexToNodeIndex index
			let joinAnalyses old new = IMap.unionWith (IMap.unionWith (+)) old new
			modify $! \st -> st {
				  graphHammerOthersAnalyses = IMap.insertWith joinAnalyses nodeIndex
					(IMap.singleton local (IMap.singleton (fromIntegral analysisIndex) incr)) $
					graphHammerOthersAnalyses st
				}

graphHammerBulkIncrementAnalysis :: Int -> VertexSet -> Int64 -> GraphHammerM as ()
graphHammerBulkIncrementAnalysis analysisIndex vertices 0 = return ()
graphHammerBulkIncrementAnalysis analysisIndex vertices incr = do
	thisNode <- liftM (fromIntegral . graphHammerNodeIndex) get
	let (left,ours,right) = IMap.splitLookup thisNode vertices
	case ours of
		-- perform increments for local analyses.
		Just vertices -> do
			forM_ (ISet.toList vertices) $ \localIndex -> do
				(sliceIndex,sliceArray) <- graphHammerGetAnalysisArrayIndex (fromIntegral analysisIndex) (fromIntegral localIndex)
				liftIO $ do
					x <- readArray sliceArray sliceIndex
					writeArray sliceArray sliceIndex (x + incr)
			modify $! \st -> st {
				  graphHammerNodesAffected = ISet.union vertices $ graphHammerNodesAffected st
				}
		Nothing -> return ()
	let joinAnalyses :: VertexSet -> OtherAnalyses -> OtherAnalyses
            joinAnalyses new old = IMap.unionWith
			(IMap.unionWith (IMap.unionWith (+)))
			(IMap.map (flip IMap.mapFromSetValue $ IMap.singleton (fromIntegral analysisIndex) incr) new)
			old
	modify $! \st -> st {
		  graphHammerOthersAnalyses = joinAnalyses left $ joinAnalyses right $
			graphHammerOthersAnalyses st
		}
	return ()

graphHammerGetOtherAnalyses :: GraphHammerM as [(Int32,AnalysesMap)]
graphHammerGetOtherAnalyses = do
	liftM (IMap.toList . graphHammerOthersAnalyses) get

graphHammerIndexToLocal :: Index -> GraphHammerM as Int32
graphHammerIndexToLocal index = do
	maxNodes <- liftM graphHammerMaxNodes get
	return $ fromIntegral $ index `div` fromIntegral maxNodes

graphHammerIndexToNodeIndex :: Index -> GraphHammerM as Int32
graphHammerIndexToNodeIndex index = do
	maxNodes <- liftM graphHammerMaxNodes get
	return $ fromIntegral $ index `mod` fromIntegral maxNodes

graphHammerCurrentNodeIndex :: GraphHammerM as Int32
graphHammerCurrentNodeIndex = liftM (fromIntegral . graphHammerNodeIndex) get

graphHammerLocalIndex :: Index -> GraphHammerM as Bool
graphHammerLocalIndex index = do
	nodeIndex <- graphHammerIndexToNodeIndex index
	liftM (nodeIndex ==) graphHammerCurrentNodeIndex

graphHammerLocalVertex :: Vertex -> GraphHammerM as Bool
graphHammerLocalVertex vertex = do
	liftM ((vertexNode vertex ==) . fromIntegral) graphHammerCurrentNodeIndex

-- |Compute "local job" flag from two vertices.
-- Properties:
--   1. local job for (v1,v2) at n1 == not (local job for (v1,v2) at n2)
--      n1 and n2 are node indices for v1 and v2.
--      an exception is for n1 is not our node and n2 is not our node too.
--   2. local job for (v1,v2) == local job for (v2,v1)
-- It is possible for those properties to do not hold for completely
-- local or completely external pair.
graphHammerLocalJob :: Vertex -> Vertex -> GraphHammerM as Bool
graphHammerLocalJob v1' v2' = do
	let n1 = vertexNode v1
	let n2 = vertexNode v2
	let randomDir = ((vertexIndex v1 `xor` vertexIndex v2) .&. 1) == 0
	v1Local <- graphHammerLocalVertex v1
	return $ (randomDir && not v1Local) || (not randomDir && v1Local)
	where
		-- order indices to support property 2.
		v1 = min v1' v2'
		v2 = max v1' v2'

graphHammerMergeIncrements :: AnalysesMap -> GraphHammerM as ()
graphHammerMergeIncrements increments = do
{-
	st <- get
	let maxNodes = graphHammerMaxNodes st
	let node = graphHammerNodeIndex st
	let header = "Merging increments at node "++show node++":"
	let prettyAIncr index (ai,incr) = concat ["    analysis [",show ai,"][",show index,"] += ",show incr]
	let prettyIndexIncrs (localI,incrs) = map (prettyAIncr (localI*maxNodes+node)) $ IMap.toList incrs
	let incrementsLines = concatMap prettyIndexIncrs $ IMap.toList increments
	let text = unlines $ header : incrementsLines
	liftIO $ putStrLn text
---}
	let flatIncrs = concatMap (\(i,as) -> map (\(ai,incr) -> (i,ai,incr)) $ IMap.toList as) $ IMap.toList increments
	forM_ flatIncrs $ \(localIndex,analysisIndex,incr) -> do
		(sliceIndex,sliceArray) <- graphHammerGetAnalysisArrayIndex (fromIntegral analysisIndex) (fromIntegral localIndex)
		liftIO $ do
			x <- readArray sliceArray sliceIndex
			writeArray sliceArray sliceIndex (x+incr)
	let mergeIncs old new = IMap.unionWith (+) old new
	modify $! \st -> st {
		  graphHammerNodesAffected = ISet.union (IMap.keysSet increments) $ graphHammerNodesAffected st
		}

graphHammerSendToNode :: Int32 -> Msg as -> GraphHammerM as ()
graphHammerSendToNode nodeIndex msg = do
	nodeChan <- liftM ((!nodeIndex) . graphHammerChannels) get
	liftIO $ writeChan nodeChan msg

graphHammerSendToNodeOfVertex :: Vertex -> Msg as -> GraphHammerM as ()
graphHammerSendToNodeOfVertex vertex msg = do
	graphHammerSendToNode (vertexNode vertex) msg 

graphHammerGroupContinuations :: Vertex -> IntSt as -> GraphHammerM as ()
graphHammerGroupContinuations vertex contState = do
	let upd v = Just $ case v of
		Just xs -> contState : xs
		Nothing -> [contState]
	modify $! \st -> st {
		  graphHammerContinuationGroups = IMap.alter
			(\v -> fmap (contState:) v `mplus` return [contState])
			(vertexNode vertex) $ graphHammerContinuationGroups st
		}

graphHammerDistributeContinuations :: GraphHammerM as ()
graphHammerDistributeContinuations = do
	nodesGroups <- liftM (IMap.toList . graphHammerContinuationGroups) get
	forM_ nodesGroups $ \(n,g) -> graphHammerSendToNode n (ContinueIntersection g)

-- |Get analyses for no more than 100 affected vertices.
graphHammerGetAffectedAnalyses :: GraphHammerM as [(Index,VertexAnalyses)]
graphHammerGetAffectedAnalyses = do
	st <- get
	let affected = take 100 $ ISet.toList $ graphHammerNodesAffected st
	let nodeIndex = fromIntegral $ graphHammerNodeIndex st
	let maxNodes = fromIntegral $ graphHammerMaxNodes st
	let toGlobal i = fromIntegral i * maxNodes + nodeIndex
	forM affected $ \localIndex -> do
		analyses <- liftM IMap.unions $ forM (assocs (graphHammerAnalyses st)) $ \(ai,analysisSlices) -> do
			(sliceIndex, slice) <- graphHammerGetAnalysisArrayIndex ai (fromIntegral localIndex)
			x <- liftIO $ readArray slice sliceIndex
			return (IMap.singleton (fromIntegral ai) x)
		return (toGlobal localIndex, analyses)

graphHammerClearAffected :: GraphHammerM as ()
graphHammerClearAffected = modify $! \st -> st { graphHammerNodesAffected = ISet.empty }

-------------------------------------------------------------------------------
-- Code that runs the analytics.

-- |Run the analyses stack.
-- Its' parameters:
--  - function to obtain edges to insert.
--  - a stack of analyses to perform.
runAnalysesStack :: (HLength as, EnabledAnalyses as as) => Integer -> Int -> IO (Maybe (UA.UArray Int Index)) ->
	Analysis as as -> IO ()
runAnalysesStack threshold maxNodes receiveChanges analysesStack
	| analysesParallelizable analysesStack = do
	putStrLn $ "Max nodes "++show maxNodes
	chans <- liftM (listArray (0,fromIntegral maxNodes-1)) $ mapM (const newChan) [0..maxNodes - 1]
	sendReceiveCountChan <- newChan
	forM_ [0..maxNodes-1] $ \n -> do
		forkIO $ workerThread analysesStack maxNodes n sendReceiveCountChan chans
	startTime <- getClockTime
	(computationPicosecs,lastCompPicosecs,lastCompCount, n) <-
		runLoop 0 0 sendReceiveCountChan chans 0 0
	endTime <- getClockTime
	let timeDiff = diffClockTimes endTime startTime
	let picoSecs = 1000000000000
	let diffPicoSecs = fromIntegral (tdSec timeDiff) * picoSecs + fromIntegral (tdPicosec timeDiff)
	let edgesPerSecond = fromIntegral n * picoSecs / diffPicoSecs
	putStrLn $ "Edges: "++show n
	putStrLn $ "Edges per second for total time: "++show edgesPerSecond
	putStrLn $ "Total time in seconds: "++show (diffPicoSecs / picoSecs)
	let computationSeconds = fromIntegral computationPicosecs / picoSecs
	putStrLn $ "Edges per second for computation time: "++show (fromIntegral n / computationSeconds)
	let lastCompSeconds = fromIntegral lastCompPicosecs / picoSecs
	putStrLn $ "Edges per second for computation time of last "++show lastCompCount++" edges: "++show (fromIntegral lastCompCount / lastCompSeconds)
	putStrLn $ "Computation time in seconds: "++show computationSeconds
	return ()
	where
		gcThreshold = threshold - 8000
		getClockPicoseconds = do
			(TOD seconds picoseconds) <- getClockTime
			return $ seconds * 1000000000000 + picoseconds
		pairs (x:y:xys) = (x,y) : pairs xys
		pairs _ = []
		runLoop time lastEdgesTime sendReceiveCountChan chans pn n = do
			edges <- liftIO receiveChanges
			case edges of
				Nothing -> do
					liftIO $ do
						answer <- newChan
						forM_ (elems chans) $ \ch -> do
							writeChan ch (Stop answer)
						forM_ (elems chans) $ \_ -> do
							readChan answer
					return (time,lastEdgesTime,n-threshold,n)
				Just edges -> do
					start <- getClockPicoseconds
					let (low,up) = UA.bounds edges
					let count = div (up-low+1) 2
					if n >= gcThreshold && n < gcThreshold + fromIntegral count
						then performGC >> putStrLn "Garbage collection."
						else return ()
					answer <- newChan
					-- seed the work.
					forM_ (elems chans) $ \ch -> writeChan ch (Portion pn edges answer)
					-- wait for answers.
--					putStrLn $ "Waiting for threads ("++show pn++")."
					hFlush stdout
					forM_ [0..maxNodes-1] $ \_ -> 
						readChan answer
					detectSilenceAndDumpState sendReceiveCountChan maxNodes (elems chans)
--					putStrLn $ "Done waiting ("++show pn++")."
					end <- getClockPicoseconds
					let delta = end - start
					let compTime = time + delta
					let lastEdgesTime'
						| n >= threshold = lastEdgesTime + delta
						| otherwise = lastEdgesTime
					runLoop compTime lastEdgesTime' sendReceiveCountChan chans (pn+1) (n+fromIntegral count)
		detectSilence countChan nodesNotSent sentReceivedBalance
			| nodesNotSent < 0 = error $ "nodesNotSent "++show nodesNotSent++"!!!"
			| nodesNotSent > 0 = continue
			| sentReceivedBalance < 0 = error $ "sentReceivedBalance "++show sentReceivedBalance++"!!!"
			| sentReceivedBalance > 0 = continue
			| otherwise = return ()
			where
				continue = do
					msg <- readChan countChan
					case msg of
						Sent _ i -> detectSilence countChan
							(nodesNotSent-1)
							(sentReceivedBalance + i)
						Received n -> detectSilence countChan
							nodesNotSent
							(sentReceivedBalance - n)
		gatherDumpAffected chans = do
			putStrLn $ "Analyses of affected indices:"
			answer <- newChan
			forM_ chans $ \ch -> writeChan ch $ GetAffected answer
			allAffected <- flip (flip foldM []) chans $ \totalAffected _ -> do
				someAffected <- readChan answer
				return $ totalAffected ++someAffected
			let firstSome = take 10 $ sort allAffected
			forM_ firstSome $ \(ix,analyses) -> do
				putStrLn $ "    Index "++show ix
				forM_ (IMap.toList analyses) $ \(ai,a) -> do
					putStrLn $ "        Analysis["++show ai++"]: "++show a
		detectSilenceAndDumpState countChan nNodes chans = do
--			putStrLn $ "Detecting silence."
			detectSilence countChan nNodes 0
--			putStrLn "All is silent."
--			gatherDumpAffected chans
		performInsertionAndAnalyses edgeList = do
			insertAndAnalyzeSimpleSequential analysesStack edgeList
runAnalysesStack _threshold _maxNodes _receiveChanges _analysesStack = do
	error "Non-parallelizable analyses aren't supported."

-- |Messages the worker thread can receive.
data Msg as = 
		-- edge changes.
		-- portion number and edges array
		Portion	!Int !(UA.UArray Int Index) (Chan ())
	|	AtomicIncrement	!Int !AnalysesMap
	|	ContinueIntersection ![IntSt as]
	|	GetAffected (Chan [(Index,IntMap Int64)])
	|	Stop (Chan Int)

-- |Counting messages sent and received.
data SendReceive =
		Sent Int Int
	|	Received Int

type MsgChan as = Chan (Msg as)

createMessageChannel :: IO (MsgChan as)
createMessageChannel = newChan

type ChanArr as = Array Int32 (MsgChan as)
type MsgChanArr as = ChanArr as

workerThread :: (HLength as, EnabledAnalyses as as) => Analysis as as ->
	Int -> Int -> Chan SendReceive -> MsgChanArr as -> IO ()
workerThread analysis maxNodes nodeIndex countingChan chans = do
	let ourChan = chans ! fromIntegral nodeIndex
	graphHammer <- graphHammerNew maxNodes nodeIndex countingChan chans
	let --receiveLoop :: EnabledAnalysis as as => Int -> GraphHammerM (Msg as) as ()
	    receiveLoop n
		| n <= 0 = return ()
		| otherwise = do
		msg <- liftIO $ readChan ourChan
		case msg of
			AtomicIncrement pn changes -> do
				graphHammerMergeIncrements changes
				graphHammerCountReceived
				receiveLoop n
			ContinueIntersection envs -> do
				forM_ envs interpret
				receiveLoop (n-length envs)
			msg -> do
{-
				liftIO $ putStrLn "resending."
--				liftIO $ hFlush stdout
---}
				liftIO $ writeChan ourChan msg
				receiveLoop n
	let --mainLoop :: EnabledAnalysis as as => GraphHammerM (Msg as) as ()
	    mainLoop = do
--		liftIO $ putStrLn $ "mainloop receiving @"++show nodeIndex
		msg <- liftIO $ readChan ourChan
		case msg of
			Portion pn edges answer -> do
--				liftIO $ putStrLn $ "Portion "++show pn++" @"++show nodeIndex
				let es = pairs $ UA.elems edges
				graphHammerFillPortionEdges es
				graphHammerClearAffected
				let work n (i,(f,t)) = do
					incr <- workOnEdge analysis i f t
					return $! n + incr
				count <- foldM work 0 $ zip [0..] es
--				liftIO $ putStrLn $ "Need to receive "++show count++" msgs @"++show nodeIndex
				graphHammerDistributeContinuations
				receiveLoop count
--				liftIO $ putStrLn $ "Sending others' increments."
				sendOtherIncrements pn
--				liftIO $ putStrLn $ "Committing portion."
				graphHammerCommitNewPortion
				liftIO $ writeChan answer ()
--				liftIO $ putStrLn $ "Sending answer."
				mainLoop
			AtomicIncrement pn changes -> do
				graphHammerMergeIncrements changes
				graphHammerCountReceived
				mainLoop
			Stop answer -> do
				liftIO $ putStrLn $ "stopped "++show nodeIndex
				liftIO $ writeChan answer nodeIndex
				return ()
			GetAffected answer -> do
				affectedAnalysis <- graphHammerGetAffectedAnalyses
				liftIO $ writeChan answer affectedAnalysis
				mainLoop
			msg -> do
				liftIO $ writeChan ourChan msg
				mainLoop
	flip runStateT graphHammer $ mainLoop
	return ()
	where
		pairs (a:b:abs) = (a,b) : pairs abs
		pairs _ = []

sendOtherIncrements :: Int -> GraphHammerM as ()
sendOtherIncrements pn = do
	increments <- graphHammerGetOtherAnalyses
--	liftIO $ putStrLn $ "Others increments: "++show increments
	forM_ increments $ \(node,incrs) -> 
		graphHammerSendToNode (fromIntegral node) (AtomicIncrement pn incrs)
	graphHammerCountSent $ length increments

workOnEdge :: Analysis as as -> Int -> Index -> Index -> GraphHammerM as Int
workOnEdge analysis edgeIndex fromIndex toIndex = do
	fromVertex <- graphHammerSplitIndex fromIndex
	toVertex <- graphHammerSplitIndex toIndex
	exists <- graphHammerEdgeExists edgeIndex fromVertex toVertex
	isFromLocal <- graphHammerLocalVertex fromVertex
	isToLocal <- graphHammerLocalVertex toVertex
	localStart <- graphHammerLocalJob fromVertex toVertex
{-
	thisNode <- graphHammerCurrentNodeIndex
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
			runStack analysis edgeIndex fromIndex toIndex
			return 0
		-- partially internal and started at our node.
		-- it sends a message and shouldn't wait.
		(_,_,True, False) -> do
			runStack analysis edgeIndex fromIndex toIndex
			return 0
		-- partially internal and started outside.
		-- should wait.
		(_,_,False, False) -> return 1
		(_,_,_, True) -> return 0
	return n

insertAndAnalyzeSimpleSequential :: Analysis as' as -> [(Index, Index)] -> GraphHammerM as ()
insertAndAnalyzeSimpleSequential stack edges =
	error "insertAndAnalyzeSimpleSequential!!!"

runStack :: Analysis as as -> Int -> Index -> Index -> GraphHammerM as ()
runStack (Analysis startV endV _ action) i start end = do
--	liftIO $ putStrLn $ "statements to interpret:"
--	liftIO $ putStrLn $ show actionStatements
	interpret (interpretInitialEnv i actionStatements)
	where
		actionStatements = ASAssign startV (cst start) : ASAssign endV (cst end) : action

-- |Is analyses stack parallelizable with our method?..
analysesParallelizable :: Analysis as' as -> Bool
analysesParallelizable (Analysis _ _ _ actions) = True

-------------------------------------------------------------------------------
-- Analysis construction monad.

class AnalysisValue v where
	toInt64 :: v -> Int64
	fromInt64 :: Int64 -> v

instance AnalysisValue Bool where
	toInt64 = fromIntegral . fromEnum
	fromInt64 = toEnum . fromIntegral

instance AnalysisValue Int where
	toInt64 = fromIntegral
	fromInt64 = fromIntegral

instance AnalysisValue Int64 where
	toInt64 = id
	fromInt64 = id

data BulkOp as where
	BulkIncr :: Int -> Value _a Index -> Value _b Index -> Value _c Int64 -> BulkOp as
	CountIncr :: (Show a, Num a, AnalysisValue a) => Value Asgn a -> Value _c a -> BulkOp as


data AnStatement as where
	-- destination and value
	ASAssign :: (Show a, AnalysisValue a) => Value Asgn a -> Value _a a -> AnStatement as
	-- start vertex for edges, end vertex for edges (will be assigned in run-time),
	-- statements to perform.
	ASOnEdges :: Value _a Index -> Value Asgn Index -> AnStatList as -> AnStatement as
	ASOnEdgesIntersection :: Value _a Index -> Value _b Index -> Value Asgn Index -> Value Asgn Index -> AnStatList as -> AnStatement as
	ASAtomicIncr :: Int -> Value _a Index -> Value _b Int64 -> AnStatement as
	ASIf :: Value _a Bool -> AnStatList as -> AnStatList as -> AnStatement as
	ASSetAnalysisResult :: Int -> Value _a Index -> Value _b Int64 -> AnStatement as
	ASFlagVertex :: Value _a Index -> AnStatement as
	ASOnFlaggedVertices :: Value Asgn Index -> AnStatList as -> AnStatement as
	ASIntersectionBulkOps :: Value _a Index -> Value _b Index -> [BulkOp as] -> AnStatement as
	ASContinueEdgeIsect :: !VertexSet -> Value Asgn Index -> Value Asgn Index -> Vertex -> AnStatList as -> AnStatement as
	ASContinueEdgeIsectBulk :: !VertexSet -> !Vertex -> [BulkOp as] -> AnStatement as

indentShow = indent . show
indent = ("    "++)
indentShowStats stats = map indent $ filter (not . null) $ concatMap (lines . show) stats
instance Show (AnStatement as) where
	show (ASAssign dest what) = show dest ++ " := "++show what
	show (ASOnEdges vertex var stats) = unlines $
		("onEdges "++show vertex++"\\"++show var) : indentShowStats stats
	show (ASOnEdgesIntersection a b aN bN stats) = unlines $
		("onEdgesIntersection "++show (a,b)++"\\"++show (aN,bN)) : indentShowStats stats
	show (ASAtomicIncr ai idx incr) = "analysisResult["++show ai++"]["++show idx++"] += "++show incr
	show (ASIf cond thens elses) = unlines $
			("if "++show cond) : "then" : indentShowStats thens ++ ("else" : map indentShow elses)
	show (ASSetAnalysisResult ai idx val) = "analysisResult["++show ai++"]["++show idx++"] := "++show val
	show (ASFlagVertex idx) = "flagVertex "++show idx
	show (ASOnFlaggedVertices x ss) = unlines $ ("onFlaggedVertices \\"++show x ++" ->") : indentShowStats ss
	showList xs = \s -> s ++ unlines (map show xs)

type AnStatList as = [AnStatement as]

data AnSt as = AnSt {
	  asValueIndex		:: !Int32
	, asStatements		:: !(AnStatList as)
	}

-- this is how we construct analyses.
type AnM as a = State (AnSt as) a

addStatement :: AnStatement as -> AnM as ()
addStatement stat = modify $! \as -> as {
	  asStatements = asStatements as ++ [stat]
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

{-
flagVertex :: Value any Index -> AnM as ()
flagVertex vertex = do
	addStatement $ ASFlagVertex vertex
-}
{-
onFlaggedVertices :: (Value Composed Index -> AnM as r) -> AnM as r
onFlaggedVertices action = do
	vertex <- defineLocal
	(eStats, r) <- cutStatements $ action $ ValueComposed vertex
	addStatement $ ASOnFlaggedVertices vertex eStats
	return r
-}

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
	idx <- getAnalysisIndex analysis
	return $ ValueComposed $ ValueAnalysisResult idx vertex

-- |Store analysis result.
putAnalysisResult :: (AnalysisIndex a as) => a -> Value _a Index -> Value _b Int64 -> AnM as ()
putAnalysisResult analysis vertex value = do
	idx <- getAnalysisIndex analysis
	addStatement $ ASSetAnalysisResult idx vertex value

-- |Update atomically result with increment.
incrementAnalysisResult :: (AnalysisIndex a as) => a -> Value _a Index -> Value _b Int64 -> AnM as ()
incrementAnalysisResult analysis vertex incr = do
	idx <- getAnalysisIndex analysis
	addStatement $ ASAtomicIncr idx vertex incr

-------------------------------------------------------------------------------
-- GraphHammer API - values and expressions.

-- value is assignable.
data Asgn
-- value is composite, thus not assignable.
data Composed

-- |A (modifable) value.
data Value asgn v where
	-- argument's index.
	ValueArgument :: Int -> Value Composed v 
	-- some local variable.
	ValueLocal :: AnalysisValue v => Int32 -> Value Asgn v
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
		ValueConst w -> show w
		ValueBin op a b -> unwords ["(",show a,")", show op, "(",show b,")"]
		ValueUn _op _a -> "unary"
		ValueComposed w -> unwords ["as_composed(",show w,")"]
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
defineLocal :: AnalysisValue v => AnM as (Value Asgn v)
defineLocal = do
	modify $! \as -> as { asValueIndex = asValueIndex as + 1 }
	liftM (ValueLocal . asValueIndex) get

-- |Define a local value and assign to it.
localValue :: (Show v, AnalysisValue v) => v -> AnM as (Value Asgn v)
localValue def = do
	v <- defineLocal
	v $= cst def
	return v

infixl 6 +., -.
(+.), (-.), (*.) :: (Show v, Num v) => Value _a v -> Value _b v -> Value Composed v
a +. b = ValueBin Plus a b
a -. b = ValueBin Minus a b
a *. b = ValueBin Mul a b
divV a b = ValueBin Div a b

(===), (=/=) :: (Show v, Eq v) => Value _a v -> Value _b v -> Value Composed Bool
a === b = ValueBin Equal a b
a =/= b = notV $ a === b

notV :: forall _a. Value _a Bool -> Value Composed Bool
notV = ValueUn Not
-- negV = ValueUn Negate

cst :: v -> Value Composed v
cst = ValueConst

-- |Assigning a value.
infixr 1 $=
($=) :: (Show v, AnalysisValue v) => Value Asgn v -> Value _a v -> AnM as ()
dest $= expr = addStatement $ ASAssign dest expr

-------------------------------------------------------------------------------
-- Interpreting analysis in GraphHammer monad.

data IntSt as = IntSt {
	  istLocals	:: !(IntMap Int64)
	, isEdgeIndex	:: !Int
	, isConts	:: ![AnStatList as]
	}

type AIM as a = StateT (IntSt as) (StateT (GraphHammer as) IO) a

interpretInitialEnv :: Int -> AnStatList as -> IntSt as
interpretInitialEnv edgeIndex actions =
	IntSt (IMap.empty) edgeIndex [actions]

interpret :: EnabledAnalyses as as => IntSt as -> GraphHammerM as ()
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
		vIndex' <- interpretValue vIndex
		lift $ graphHammerIncrementAnalysis aIndex vIndex' incr
	ASIf cond thenStats elseStats -> do
		c <- interpretValue cond
		let stats = if c then thenStats else elseStats
		modify $! \st -> st { isConts = stats : isConts st }
	ASContinueEdgeIsect edgeSet a b thisNodeVertex onEdgeStats -> do
		ei <- liftM isEdgeIndex get
		thisEdgeSet <- lift $ graphHammerGetEdgeSet ei thisNodeVertex
		isection <- lift $ graphHammerVertexSetIntersectionAsIndices edgeSet thisEdgeSet
		let cont c = ASAssign a (cst c) : ASAssign b (cst c) :
			onEdgeStats
		modify $! \st -> st { isConts = map cont isection ++ isConts st }
	ASContinueEdgeIsectBulk edgeSet thisNodeVertex bulkOps -> do
		ei <- liftM isEdgeIndex get
		thisEdgeSet <- lift $ graphHammerGetEdgeSet ei thisNodeVertex
		isection <- lift $ graphHammerVertexSetIntersection edgeSet thisEdgeSet
		interpretBulkOps isection bulkOps
	ASOnEdgesIntersection av bv aN bN stats -> do
		interpretEdgesIntersection av bv aN bN stats
	ASIntersectionBulkOps av bv bulkStats ->
		interpretIntersectionBulkOps av bv bulkStats

assignValue :: Show v => Value Asgn v -> Value _b v -> AIM as ()
assignValue (ValueLocal idx) what = do
	x <- interpretValue what
	modify $! \ist -> ist { istLocals = IMap.insert idx (toInt64 x) $ istLocals ist }

interpretIntersectionBulkOps :: Value _a Index -> Value _b Index -> [BulkOp as] -> AIM as ()
interpretIntersectionBulkOps a b ops = do
	ei <- liftM isEdgeIndex get
	s1 <- interpretVertexValue a
	s2 <- interpretVertexValue b
	l1 <- lift $ graphHammerLocalVertex s1
	l2 <- lift $ graphHammerLocalVertex s2
	case (l1,l2) of
		-- this is filtered out in previous steps.
		(False, False) -> error "completely non-local computation!"
		-- completely local computation.
		(True, True) -> do
			e1 <- lift $ graphHammerGetEdgeSet ei s1
			e2 <- lift $ graphHammerGetEdgeSet ei s2
			isection <- lift $ graphHammerVertexSetIntersection e1 e2
			interpretBulkOps isection ops
		-- partially local computations that started in our node.
		(True, False) -> do
			ourEdges <- lift $ graphHammerGetEdgeSet ei s1
			sendAndStop s2 ourEdges
		(False, True) -> do
			ourEdges <- lift $ graphHammerGetEdgeSet ei s2
			sendAndStop s1 ourEdges
	where
		sendAndStop destIndex ourEdges = do
			st <- get
			let continueStat = ASContinueEdgeIsectBulk ourEdges destIndex ops
			let sendSt = continueStat `seq` st { isConts = [continueStat] : isConts st }
			lift $ graphHammerGroupContinuations destIndex $! sendSt
			-- stop interpreting here. It will be continued on another node.
			modify $! \st1 -> st1 { isConts = [] }

interpretBulkOps :: VertexSet -> [BulkOp as] -> AIM as ()
interpretBulkOps _isection [] = return ()
interpretBulkOps isection (op:ops) = do
	case op of
		BulkIncr aindex _ _ incr -> do
			v <- interpretValue incr
			lift $ graphHammerBulkIncrementAnalysis aindex isection v
		CountIncr v incr -> do
			assignValue v (incr *. cst (fromIntegral $ vertexSetSize isection))
	interpretBulkOps isection ops

interpretEdgesIntersection :: Value _a Index -> Value _b Index -> Value Asgn Index -> Value Asgn Index -> AnStatList as -> AIM as ()
interpretEdgesIntersection a b aN bN stats = do
	ei <- liftM isEdgeIndex get
	s1 <- interpretVertexValue a
	s2 <- interpretVertexValue b
	l1 <- lift $ graphHammerLocalVertex s1
	l2 <- lift $ graphHammerLocalVertex s2
	case (l1,l2) of
		(False, False) -> error "completely non-local computation!"
		(True,True) -> do
			e1 <- lift $ graphHammerGetEdgeSet ei s1
			e2 <- lift $ graphHammerGetEdgeSet ei s2
			let cont c = ASAssign aN (cst c) :
				ASAssign bN (cst c) :
				stats
			isection <- lift $ graphHammerVertexSetIntersectionAsIndices e1 e2
			let conts = map cont isection
			modify $! \st -> st { isConts = conts ++ isConts st }
		-- one is local to us, another is out of our reach.
		(True,False) -> do
			ourEdges <- lift $ graphHammerGetEdgeSet ei s1
			sendAndStop s2 s1 ourEdges
		(False,True) -> do
			ourEdges <- lift $ graphHammerGetEdgeSet ei s2
			sendAndStop s1 s2 ourEdges
	where
		sendAndStop destIndex _ourIndex ourEdges = do
			st <- get
			let continueStat = ASContinueEdgeIsect ourEdges aN bN destIndex stats
			let sendSt = st { isConts = [continueStat] : isConts st }
			lift $ graphHammerGroupContinuations destIndex sendSt
			-- stop interpreting here. It will be continued on another node.
			modify $! \st1 -> st1 { isConts = [] }

interpretOnEdges :: EnabledAnalyses as as => Value _a Index -> Value Asgn Index -> AnStatList as -> AIM as ()

-- special case for edge sets intersection.
interpretOnEdges startVertex1 vertexToAssign1@(ValueLocal i1)
	[ASOnEdges startVertex2 vertexToAssign2@(ValueLocal i2) [ASIf (ValueBin Equal a b) thenStats []]]
	| Just i3 <- uncompose a
	, Just i4 <- uncompose b
	, (i1 == i3 && i2 == i4) || (i1 == i4 && i2 == i3) =
		interpretEdgesIntersection startVertex1 startVertex2 vertexToAssign1 vertexToAssign2 thenStats

interpretOnEdges _startVertex _vertexToAssign _stats = do
	error "standalone onEdges is not supported right now!"

interpretValue :: Value _a v -> AIM as v
interpretValue value = case value of
	ValueLocal index1 -> do
		locals <- liftM istLocals get
		case IMap.lookup index1 locals of
			Just v -> return (fromInt64 v)
			Nothing -> error $ "local variable #"++show index1++" not found in "++show locals++"."
	ValueArgument _index -> error "interpreting ValueArgument!!!"
	ValueConst v -> return v
	ValueBin Plus l r -> interpretBin (+) l r
	ValueBin Minus l r -> interpretBin (-) l r
	ValueBin Mul l r -> interpretBin (*) l r
	ValueBin Div l r -> interpretBin (div) l r
	ValueBin Equal l r -> interpretBin (==) l r
	ValueUn Not val -> liftM not $ interpretValue val
	ValueUn Negate val -> liftM negate $ interpretValue val
	ValueComposed v -> interpretValue v
	ValueAnalysisResult analysisIndex1 vertex -> do
		v <- interpretValue vertex
		lift $ graphHammerGetAnalysis analysisIndex1 v
	where
		interpretBin :: (a -> b -> r) -> Value _a a -> Value _b b -> AIM as r
		interpretBin f a b = liftM2 f (interpretValue a) (interpretValue b)

interpretVertexValue :: Value _a Index -> AIM as Vertex
interpretVertexValue value = do
	i <- interpretValue value
	lift $ graphHammerSplitIndex i

-------------------------------------------------------------------------------
-- Optimizing statements operations.

optimizeStatements :: AnStatList as -> AnStatList as
-- trivial case.
optimizeStatements [] = []
-- important case of edge intesection.
optimizeStatements (stat : stats)
	| Just stats' <- recognizeOptimizeIntersection stat
		= stats' ++ optimizeStatements stats
-- all other cases aren't optimizable.
optimizeStatements (stat : stats) = stat : optimizeStatements stats

recognizeOptimizeIntersection :: AnStatement as ->
	Maybe (AnStatList as)
recognizeOptimizeIntersection stat = do
	isection <- recognizeIntersection stat
	let isections = optimizeIntersection isection
	return isections

optimizeIntersection :: AnStatement as -> [AnStatement as]
optimizeIntersection stat@(ASOnEdgesIntersection a b aN bN stats) = case stats of
	[ASAtomicIncr anIx ix incr, ASAssign v (ValueBin Plus l r)]
		| incrIsConst incr
		, Just iix <- uncompose ix
		, Just iaN <- uncompose aN
		, Just ibN <- uncompose bN
		, iix == iaN || iix == ibN
		, Just iv <- uncompose v
		, Just assignIncr <- uncomposeOne iv l r ->
			[ ASIntersectionBulkOps a b [BulkIncr anIx a b incr, CountIncr v assignIncr]]
	_ -> [stat]
	where
		uncomposeOne :: Int32 -> Value _a x -> Value _b x -> Maybe (Value Composed x)
		uncomposeOne rq a1 b1 = do
				i <- uncompose a1
				if i == rq then return (castComposed b1) else mzero
			`mplus` do
				i <- uncompose b1
				if i == rq then return (castComposed a1) else mzero
		castComposed :: Value _a a -> Value Composed a
		castComposed (ValueComposed v) = ValueComposed v
		castComposed (ValueConst c) = ValueConst c
		castComposed v = ValueComposed v
		incrIsConst :: Value _a c -> Bool
		incrIsConst (ValueConst _c) = True
		incrIsConst _ = False
optimizeIntersection _ = error "Not an intersection interation operator."

uncompose :: Value _a x -> Maybe Int32
uncompose (ValueComposed a) = uncompose a
uncompose (ValueLocal i) = Just i
uncompose _ = Nothing

recognizeIntersection :: AnStatement as -> Maybe (AnStatement as)
recognizeIntersection (ASOnEdges a aN [ASOnEdges b bN [ASIf cond stats []]]) = do
	_ <- case cond of
		ValueBin Equal x y -> do
			ix <- uncompose x
			iy <- uncompose y
			iaN <- uncompose aN
			ibN <- uncompose bN
			if (ix == iaN && iy == ibN) || (ix == ibN && iy == iaN)
				then return undefined
				else mzero
		_ -> mzero
	return $ ASOnEdgesIntersection a b aN bN stats
recognizeIntersection _ = mzero

-------------------------------------------------------------------------------
-- GraphHammer analysis combination.

type family RequiredAnalyses a

-- Removed for feature use
-- data AnalysisNotEnabled a

class EnabledAnalysis a as

instance EnabledAnalysis a (a :. as)
instance EnabledAnalysis a as => EnabledAnalysis a (a' :. as)

class EnabledAnalyses as eas

instance EnabledAnalyses Nil eas
instance (EnabledAnalyses as eas, EnabledAnalysis a eas) => EnabledAnalyses (a :. as) eas

data Analysis as wholeset where
	Analysis :: (EnabledAnalyses (RequiredAnalyses a) as, EnabledAnalyses as wholeset, EnabledAnalyses (a :. as) wholeset) =>
			Value Asgn Index -> Value Asgn Index -> Int32 ->
			AnStatList (a :. as) -> Analysis (a :. as) wholeset

basicAnalysis :: ((RequiredAnalyses a) ~ Nil, EnabledAnalysis a wholeset) =>
	a -> (a -> Value Composed Index -> Value Composed Index -> AnM (a :. Nil) ()) -> Analysis (a :. Nil) wholeset
basicAnalysis analysis edgeInsert =
	Analysis sv ev i stats
	where
		i = asValueIndex env
		stats = optimizeStatements $ asStatements env
		((sv,ev),env) = flip runState (AnSt 0 []) $ do
			sv1 <- defineLocal
			ev1 <- defineLocal
			edgeInsert analysis (ValueComposed sv1) (ValueComposed ev1)
			return (sv1,ev1)

derivedAnalysis :: (EnabledAnalyses (RequiredAnalyses a) as, EnabledAnalyses as wholeset, EnabledAnalyses (a :. as) wholeset)  =>
                Analysis as wholeset -> a -> (a -> Value Composed Index -> Value Composed Index -> AnM (a :. as) ()) -> Analysis (a :. as) wholeset
derivedAnalysis (Analysis startV endV startI requiredActions) analysis edgeInsert =
    Analysis startV endV i (map liftStatement requiredActions ++ currentActions)
  where
    initialState = AnSt startI []
    liftStatement :: AnStatement as -> AnStatement (a :. as)
    liftStatement stat = case stat of
        ASAssign v e -> ASAssign v e
        ASOnEdges i1 arg as -> ASOnEdges i1 arg (map liftStatement as)
        ASAtomicIncr ai vi incr -> ASAtomicIncr ai vi incr
        ASIf cond thens elses -> ASIf cond (map liftStatement thens) (map liftStatement elses)
        ASSetAnalysisResult ai vi val -> ASSetAnalysisResult ai vi val
        ASFlagVertex v -> ASFlagVertex v
        ASOnFlaggedVertices arg stats -> ASOnFlaggedVertices arg $ map liftStatement stats
        -- new ones
        ASOnEdgesIntersection va vb ai bi stats -> ASOnEdgesIntersection va vb ai bi $ map liftStatement stats
--        ASIntersectionBulkOps va vb ops -> ASIntersectionBulkOps va vb ops
        ASContinueEdgeIsect vs ai bi v stat -> ASContinueEdgeIsect vs ai bi v $ map liftStatement stat
--        ASContinueEdgeIsectBulk _ _ _
    currentActions = optimizeStatements $ asStatements env
    i = asValueIndex env
    env = flip execState initialState $ do
            edgeInsert analysis (ValueComposed startV) (ValueComposed endV)

class EnabledAnalysis a as => AnalysisIndex a as where
	analysisIndex :: a -> as -> Int

instance AnalysisIndex a as => AnalysisIndex a (a' :. as) where
	analysisIndex a list = analysisIndex a (hTail list)
instance HLength as => AnalysisIndex a (a :. as) where
	analysisIndex _ list = hLength (hTail list)

{-
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
-}
