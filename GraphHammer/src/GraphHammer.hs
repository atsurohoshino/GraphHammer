-- |STINGER.hs
-- "Spatio-Temporal Interaction Networks and Graphs (STING) Extensible Representation"
-- Top-level module that exports all things STINGER related.

module GraphHammer(
	  module GraphHammer.Info
	-- Selecting implementation to export.
	, module GraphHammer.SimplestParallel
	--, module STINGER.Simplest
	) where

-- Information storage and retrieval.
import GraphHammer.Info

-- Prototype implementation.
import GraphHammer.SimplestParallel
