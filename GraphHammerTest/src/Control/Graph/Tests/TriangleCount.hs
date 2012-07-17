-- |TriangleCount
--
-- A test for LanGra that implements the famous triangle count analysis.
--
-- Copyright (C) 2012 Parallel Scientific

{-# LANGUAGE DeriveDataTypeable #-}

module Main where

import Control.Monad
import Data.Char
import Data.List

import LanGra

data TriangleCount = TriangleCount deriving (Show, Typeable)

-------------------------------------------------------------------------------
-- Analysis code.

-- |Count triangles for all vertices after loading a graph.
afterLoad :: AnM ()
afterLoad = do
	count <- localValue (cst 0)
	onAffected $ \vertex -> do
		onEdges vertex $ \neighbor1 -> do
			onEdges vertex $ \neighbor2 -> do
				anWhen (neighbor1 =/= neighbor2)
					(onEdgeIntersection neighbor1 neighbor2 $ \n3 -> anWhen (n3 =/= vertex) (count $= count +. cst 1))
		putAnalysisResult TriangleCount vertex (count `divV` cst 2)

onEdgeIntersection :: Value _a Index -> Value _b Index -> (Value Comp Index -> AnM ()) -> AnM ()
onEdgeIntersection f t act = do
	onEdges f $ \n1 -> onEdges t $ \n2 -> do
		anWhen (n1 === n2) (act n1)

-- |And a dynamic update reaction.
triangleCountUpdate :: Value Comp Bool -> Value Comp Index -> Value Comp Index -> AnM ()
triangleCountUpdate added f t = do
	dir <- localValue (cst 1)
	anWhen (notV added) (dir $= cst (-1))
	count <- localValue (cst 0)
	onEdgeIntersection f t $ \n -> do
		count $= count +. dir
		incrementAnalysisResult TriangleCount n dir
	incrementAnalysisResult TriangleCount f count
	incrementAnalysisResult TriangleCount t count

-------------------------------------------------------------------------------
-- Simple test harness.

-- |Various test cases to test
test :: Int -> Int -> Int -> IO ()
test n m ps = do
	putStrLn $ "Testing with number of vertices "++show n++", number of neighbours - 1 "++show m++", and update portion size "++show ps++"."
	putStrLn $ "Graph "++show graph
	putStrLn $ "Updates "++show updates
	runAn afterLoad triangleCountUpdate Nothing graph updates
	where
		graph = [(i,i+1) | i <- map fromIntegral [0..n-2]]
		split [] = []
		split xs = portion : split rest
			where
				(portion,rest) = splitAt ps xs
		updates :: Updates
		updates = split [((i,min (fromIntegral n-1) (i+d)), True) | i <- map fromIntegral [0..n-1], d <- map fromIntegral [1..m]]

-------------------------------------------------------------------------------
-- "Twitter" test harness.

type Text = [String]
type TextWords = [[String]]

meaninglessWords = ["a", "to", "the", "is", "are"]
meaninglessWord word = word `elem` meaninglessWords

noPunct :: Text -> Text
noPunct lines = map (map toLower) $ map (filter notPunct) lines
	where
		notPunct c = not $ c `elem` "!,;:"

toWords :: Text -> TextWords
toWords lines = map (filter (not . meaninglessWord)) $ map words $ noPunct lines

toBeOrNotToBe :: TextWords
toBeOrNotToBe = toWords [
	  "To be, or not to be: that is the question:"
	, "Whether 'tis nobler in the mind to suffer"
	, "The slings and arrows of outrageous fortune,"
	, "Or to take arms against a sea of troubles,"
	, "And by opposing end them? To die: to sleep;"
	, "No more; and by a sleep to say we end"
	, "The heart-ache and the thousand natural shocks"
	, "That flesh is heir to, 'tis a consummation"
	, "Devoutly to be wish'd. To die, to sleep;"
	, "To sleep: perchance to dream: ay, there's the rub;"
	, "For in that sleep of death what dreams may come"
	, "When we have shuffled off this mortal coil,"
	, "Must give us pause: there's the respect"
	, "That makes calamity of so long life;"
	, "For who would bear the whips and scorns of time,"
	, "The oppressor's wrong, the proud man's contumely,"
	, "The pangs of despised love, the law's delay,"
	, "The insolence of office and the spurns"
	, "That patient merit of the unworthy takes,"
	, "When he himself might his quietus make"
	, "With a bare bodkin? who would fardels bear,"
	, "To grunt and sweat under a weary life,"
	, "But that the dread of something after death,"
	, "The undiscover'd country from whose bourn"
	, "No traveller returns, puzzles the will"
	, "And makes us rather bear those ills we have"
	, "Than fly to others that we know not of?"
	, "Thus conscience does make cowards of us all;"
	, "And thus the native hue of resolution"
	, "Is sicklied o'er with the pale cast of thought,"
	, "And enterprises of great pith and moment"
	, "With this regard their currents turn awry,"
	, "And lose the name of action. - Soft you now!"
	, "The fair Ophelia! Nymph, in thy orisons"
	, "Be all my sins remember'd."]

socketPacketPocket :: TextWords
socketPacketPocket = toWords [
	  "    Here's an easy game to play."
	, "    Here's an easy thing to say:"
	, ""
	, "    If a packet hits a pocket on a socket on a port,"
	, "    And the bus is interrupted as a very last resort,"
	, "    And the address of the memory makes your floppy disk abort,"
	, "    Then the socket packet pocket has an error to report!"
	, ""
	, "    If your cursor finds a menu item followed by a dash,"
	, "    And the double-clicking icon puts your window in the trash,"
	, "    And your data is corrupted 'cause the index doesn't hash,"
	, "    Then your situation's hopeless, and your system's gonna crash!"
	, ""
	, "    You can't say this?"
	, "    What a shame sir!"
	, "    We'll find you"
	, "    Another game sir."
	, ""
	, "    If the label on the cable on the table at your house,"
	, "    Says the network is connected to the button on your mouse,"
	, "    But your packets want to tunnel on another protocol,"
	, "    That's repeatedly rejected by the printer down the hall,"
	, ""
	, "    And your screen is all distorted by the side effects of gauss"
	, "    So your icons in the window are as wavy as a souse,"
	, "    Then you may as well reboot and go out with a bang,"
	, "    'Cause as sure as I'm a poet, the sucker's gonna hang!"
	, ""
	, "    When the copy of your floppy's getting sloppy on the disk,"
	, "    And the microcode instructions cause unnecessary risc,"
	, "    Then you have to flash your memory and you'll want to RAM your ROM."
	, "    Quickly turn off the computer and be sure to tell your mom!"
	]

testText :: String -> TextWords -> IO ()
testText msg textWords = do
	putStrLn $ "As if poets tweetted: "++msg
	runAn afterLoad triangleCountUpdate (Just indexToWord) [] updates
	return ()
	where
		allWords :: [String]
		allWords = nub $ concat textWords
		wordIndex :: String -> Index
		wordIndex s = case elemIndex s allWords of
			Just i -> fromIntegral i
			Nothing -> error $ "word "++show s++" is not in the allWords list."
		indexToWord :: Index -> String
		indexToWord i = allWords !! fromIntegral i
		makeUpdateFromWords words = [((i,j), True) | i <- indices, j <- indices]
			where
				indices = map wordIndex words
		updates = map makeUpdateFromWords textWords

-------------------------------------------------------------------------------
-- Running them all in executable.

main = do
	let n = 3
	sequence_ [test n m ps | m <- [1,2], ps <- [5{-,10,20-}]]
	testText "To be or not to be" toBeOrNotToBe
	testText "Socket packet pocket" socketPacketPocket