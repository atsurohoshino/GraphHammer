-- |GraphHammer.VertexDegree
-- Copyright : (C) 2013 Parallel Scientific Labs, LLC.
-- License   : GPLv2
--
--
-- Vertex degree analysis.

{-# LANGUAGE TypeFamilies, TypeOperators, FlexibleInstances, MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleContexts #-}

module GraphHammer.VertexDegree(
	  VertexDegree(..)
	, vertexDegree
	) where

import GraphHammer

------------------------------------------------------------------------------------
-- Typical basic analysis.

data VertexDegree = VertexDegree

type instance RequiredAnalyses VertexDegree = Nil

vertexDegree :: EnabledAnalysis VertexDegree wholeset => Analysis (VertexDegree :. Nil) wholeset
vertexDegree = basicAnalysis VertexDegree $ \a from to -> do
	incrementAnalysisResult a from (cst 1)
	incrementAnalysisResult a to (cst 1)
	return ()
