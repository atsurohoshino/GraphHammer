-- |STINGER.hs
-- "Spatio-Temporal Interaction Networks and Graphs (STING) Extensible Representation"
-- Top-level module that exports all things STINGER related.

module STINGER(
	  module STINGER.Info
	-- Selecting implementation to export.
	, module STINGER.SimplestParallel
	--, module STINGER.Simplest
	) where

-- Information storage and retrieval.
import STINGER.Info

-- Prototype implementation.
import STINGER.SimplestParallel
