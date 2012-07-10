{-# LANGUAGE TypeFamilies #-}
-- |Control.Graph.Interface
--
-- Absract interface to high performance graph queries.
--
-- Copyright (C) 2012 Parallel Scientific

module Control.Graph.Interface(
	Interface(..),
	ActionType) where

import Data.Typeable (Typeable)
import Data.Int (Int64)
import Data.Array.Unboxed
 
import LanGra
import LanGra.Interpret

type ActionType = IntSt -> IO IntSt

class Interface a where
	-- |Handle that we use to work with queries.
	-- This handle contains channel that is used to send messages to processing
	-- thread.
	-- This way caller can use any monad that supports @liftIO@ to call graph
	-- processing routines.
	data QHandle a :: *

	-- |Version string. Use it to indicate what implementation you're using.
	-- Mainly interesting to time different implementation.
	versionString :: a -> String

	-- |Open a new query processor, with empty graph.
	newQHandle :: a -> IO (QHandle a)

	-- |Close a query procesor.
	closeQHandle :: QHandle a -> IO ()

	-- |Add edges to a graph. We only *add* edges here. Negative values aren't allowed here.
	-- Such values signify deletion of edge.
	addEdges :: QHandle a -> UArray Int Index -> IO ()

	-- |Run analysis on whole graph. Analytics results are cleared before code is run.
	-- We have an official channel to read data from analysis - @getAnalysisResult@.
	-- Use it after @runAnalysis@.
	runAnalysis :: QHandle a -> AnM () -> IO ()

	-- |Add edges to a graph
	addAnalyzeEdges :: QHandle a -> UpdateAnalysis -> UArray Int Index -> IO ()

	-- |Add edges to a graph.
	getAnalysisResult :: Typeable t => QHandle a -> t -> Index -> IO Int64

	-- Get maximum vertex index.	
	getMaxVertexIndex :: QHandle a -> IO Index
