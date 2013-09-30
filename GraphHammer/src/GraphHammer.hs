-- | GraphHammer.hs
-- Copyright : (C) 2013 Parallel Scientific Labs, LLC.
-- License   : GPLv2
--
-- Top level module for GraphHammer library
module GraphHammer(
	  module GraphHammer.Info
	-- Selecting implementation to export.
	, module GraphHammer.SimplestParallel
	) where

-- Information storage and retrieval.
import GraphHammer.Info

-- Prototype implementation.
import GraphHammer.SimplestParallel
