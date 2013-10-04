{-# LANGUAGE TypeFamilies, EmptyDataDecls, TypeOperators, FlexibleInstances, MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleContexts #-}
-- | 
-- Module    : GraphHammer.TriangleCount
-- Copyright : (C) 2013 Parallel Scientific Labs, LLC.
-- License   : GPLv2
--
--
-- Computing clustering coefficient (local and global) using GraphHammer.
module GraphHammer.TriangleCount(
	  TriangleCount(..)
	, triangleCount
	) where


import GraphHammer

import GraphHammer.VertexDegree

-------------------------------------------------------------------------------
-- Typical derived analysis.
-- Closed triangle count.

-- |The name of analysis.
data TriangleCount = TriangleCount

-- |What analyses are required to be done before ours.
type instance RequiredAnalyses TriangleCount = VertexDegree :. Nil

triangleCount :: (EnabledAnalyses (TriangleCount :. VertexDegree :.Nil) wholeset
	, EnabledAnalysis VertexDegree wholeset) =>
	Analysis (TriangleCount :. VertexDegree :. Nil) wholeset
triangleCount = derivedAnalysis vertexDegree TriangleCount $ \tc from to -> do
	count <- localValue 0
	onIntersectingNeighbors from to $ \neighbor -> do
		-- atomically update both nodes neighbor's triangle count.
		incrementAnalysisResult tc neighbor (cst 1)
		count $= count +. cst 1
	-- atomically update ours triangle count.
	incrementAnalysisResult tc from count
	incrementAnalysisResult tc to   count
	return ()

onIntersectingNeighbors :: Value Composed Index -> Value Composed Index
	-> (Value Composed Index -> AnM as ()) -> AnM as ()
onIntersectingNeighbors u v action = do
	onEdges u $
		\x -> onEdges v $
			\y -> anIf (x === y) (action x) (return ())
