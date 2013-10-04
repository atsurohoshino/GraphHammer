{-# LANGUAGE TypeFamilies, EmptyDataDecls, TypeOperators, FlexibleInstances, MultiParamTypeClasses, FlexibleContexts #-}
-- | 
-- Module    : GraphHammer.ClusteringCoefficient
-- Copyright : (C) 2013 Parallel Scientific Labs, LLC.
-- License   : GPLv2
-- Stability : unstable
--
-- Clustering coefficient computation, this module is still in experimental
-- state, and requires updates for full featured use.
module GraphHammer.ClusteringCoefficient(
	  ClusteringCoefficient(..)
	, clusteringCoefficient
	) where

import GraphHammer
import GraphHammer.TriangleCount
import GraphHammer.VertexDegree

-- | ClusteringCoefficient analysis tag
data ClusteringCoefficient = ClusteringCoefficient

type instance RequiredAnalyses ClusteringCoefficient = TriangleCount :. VertexDegree :. Nil

-- | ClusteringCoefficient analysis
clusteringCoefficient :: (EnabledAnalysis ClusteringCoefficient wholeset
                         , EnabledAnalysis TriangleCount wholeset
                         , EnabledAnalysis VertexDegree wholeset)
                      => Analysis (ClusteringCoefficient :. TriangleCount :. VertexDegree :. Nil) wholeset
clusteringCoefficient = derivedAnalysis triangleCount ClusteringCoefficient $ \cc from to -> do
	let update index = do
		tc <- getAnalysisResult TriangleCount index
		deg <- getAnalysisResult VertexDegree index
		putAnalysisResult cc index (divV (tc *. cst 100) (deg *. (deg -. cst 1)))
	update from
	update to
