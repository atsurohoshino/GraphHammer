-- |STINGER.VertexDegree
--
-- Vertex degree analysis.

{-# LANGUAGE TypeFamilies, TypeOperators, FlexibleInstances, MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleContexts #-}

module STINGER.VertexDegree(
	  VertexDegree(..)
	, vertexDegree
	) where

import STINGER

------------------------------------------------------------------------------------
-- Typical basic analysis.

data VertexDegree = VertexDegree

type instance RequiredAnalyses VertexDegree = Nil

vertexDegree :: EnabledAnalysis VertexDegree wholeset => Analysis (VertexDegree :. Nil) wholeset
vertexDegree = basicAnalysis VertexDegree $ \a from to -> do
	incrementAnalysisResult a from (cst 1)
	incrementAnalysisResult a to (cst 1)
	return ()
