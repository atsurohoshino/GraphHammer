-- | GraphHammer.ClusteringCoefficient
-- Copyright : (C) 2013 Parallel Scientific Labs, LLC.
-- License   : GPLv2
--
-- Clustering coefficient computation.

{-# LANGUAGE TypeFamilies, EmptyDataDecls, TypeOperators, FlexibleInstances, MultiParamTypeClasses #-}

module GraphHammer.ClusteringCoefficient(
	  ClusteringCoefficient(..)
	, clusteringCoefficient
	) where

import GraphHammer
import GraphHammer.TriangleCount
import GraphHammer.VertexDegree

data ClusteringCoefficient = ClusteringCoefficient

type instance RequiredAnalyses ClusteringCoefficient = TriangleCount :. VertexDegree :. Nil


clusteringCoefficient :: Analysis (ClusteringCoefficient :. TriangleCount :. VertexDegree :. Nil)
clusteringCoefficient = derivedAnalysis triangleCount $ \cc from to -> do
	let update index = do
		tc <- getAnalysisResult TriangleCount index
		deg <- getAnalysisResult VertexDegree index
		putAnalysisResult cc index (divV (tc *. cst 100) (deg *. (deg -. cst 1)))
	update from
	update to
