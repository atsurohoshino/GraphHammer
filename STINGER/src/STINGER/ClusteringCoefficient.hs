-- |STINGER.ClusteringCoefficient
--
-- Clustering coefficient computation.

{-# LANGUAGE TypeFamilies, EmptyDataDecls, TypeOperators, FlexibleInstances, MultiParamTypeClasses #-}

module STINGER.ClusteringCoefficient(
	  ClusteringCoefficient(..)
	, clusteringCoefficient
	) where

import STINGER
import STINGER.TriangleCount
import STINGER.VertexDegree

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
