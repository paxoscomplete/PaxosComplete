{-# LANGUAGE FlexibleInstances, MultiParamTypeClasses, ExistentialQuantification #-}

module Paxos where
import Control.Monad.State

import Data.List as List
import Data.Set as Set
import Data.Map as Map
import Data.IntMap as IntMap
import Test.QuickCheck
import Test.SmallCheck
import Test.SmallCheck.Series
import Test.SmallCheck.Drivers


-------------------------------
data NodeID = Nobody
            | NodeA  | NodeB | NodeC
            | NodeD  | NodeE | NodeF  
            | NodeG  | NodeH | NodeI
            | MaxNodeID  deriving (Eq, Ord, Show, Read)
data Value  = NoValue | NoOp | ValueA | ValueB | ValueC deriving (Eq, Ord, Show, Read)
data Round  = Round Int NodeID deriving (Eq, Ord, Show)
data Vote   = Vote Round Value deriving (Eq, Show)

-------------------------------
data From = From NodeID deriving (Show)
data To   = To   NodeID deriving (Show)

-------------------------------
zero       = Round 0 Nobody
one        = Round 1 Nobody
blank      = Vote zero NoValue
noop       = Vote one  NoOp

-------------------------------
leaderIDs   = [NodeA, NodeB, NodeC]  
acceptorIDs = [NodeD, NodeE, NodeF]
learnerIDs  = [NodeG, NodeH, NodeI]

----------------------------
---- Utility Functions  ----
----------------------------
promoteRound (Round a _) nid = Round (a+1) nid

chooseHighest   (Vote ar _) b@(Vote br _) | ar < br = b     -- A more Haskell-like method would be to override the Ord instance.  Longhand here to show it is not magic.
chooseHighest a@(Vote ar _)   (Vote br _) | ar > br = a
chooseHighest   (Vote ar a)   (Vote br b) | a /= b  = error $ "INCONSISTENCY DETECTED: " ++ (show ar) ++ ":" ++ (show a) ++ " " ++ (show br) ++ ":" ++ (show b)  -- Inline error detection..
chooseHighest a             _                       = a

chooseProposal r (Vote _ v) p | v == NoValue = Vote r p     -- Empty slot gets filled with the proposed value
chooseProposal r (Vote _ v) _ | otherwise    = Vote r v     -- Pre-existing value gets re-proposed.

fillHoles      r (Vote _ v)   | v == NoValue = Vote r NoOp  -- Empy slot gets filled with NoOp
fillHoles      r (Vote _ v)   | otherwise    = Vote r v     -- Pre-existing value gets re-proposed.

maxInstance m = if IntMap.null m then 0 else case IntMap.findMax m of (a,_) -> a
minInstance m = if IntMap.null m then 0 else case IntMap.findMin m of (a,_) -> a

quorumAchieved needed quorum = Set.size quorum >= needed

forwardRequest _   _    (Round _ Nobody) = Nothing
forwardRequest req myID (Round _ leader) | myID == leader = Nothing
forwardRequest req myID (Round _ _)      | otherwise      = Just $ req

fillWithNoOps :: IntMap Vote -> Int -> Round -> IntMap Vote
fillWithNoOps m high r =
  let low   = minInstance m
      noops = Prelude.foldl (\a b -> IntMap.insert b (Vote r NoOp) a) IntMap.empty [low..high]
  in IntMap.union m noops

quorumLowest :: Int -> Map NodeID Int -> Int
quorumLowest quorum ls = case drop (quorum - 1) . List.sort . Map.elems $ ls of [] -> 0; (a:_) -> a

----------------------------
---- Test Harness Stuff ----
----------------------------
data Event
  = Deliver
  | Duplicate
  | Drop
  | Shift
  | Tick NodeID
  | Req NodeID Value
  deriving (Read, Show)

-------------------------------
instance Arbitrary NodeID where
   arbitrary = elements leaderIDs

instance Arbitrary Value where
   arbitrary = elements [ValueA,ValueB,ValueC] --,ValueD,ValueE]

instance Arbitrary Event where
   arbitrary = do
      node <- arbitrary
      val  <- arbitrary
      frequency
        [ (50, elements [Deliver]) 
        , (40, elements [Drop])
        , (20, elements [Tick node])
        , (20, elements [Req node val])
        , (5, elements [Shift])
        , (5, elements [Duplicate])
        ]

-------------------------------
instance (Monad m) => Serial m Value where
  series = cons0 ValueA \/ cons0 ValueB \/ cons0 ValueC -- Add more here to test additional breadth

instance (Monad m) => Serial m NodeID where
  series = cons0 NodeA \/ cons0 NodeB \/ cons0 NodeC -- \/ cons0 NodeC -- Add more here to test additional breadth

instance (Monad m) => Serial m Event where
  series = cons0 Deliver \/ cons0 Drop -- \/ cons1 Tick \/ cons2 Req -- \/ cons0 Shift \/ cons0 Duplicate 

-------------------------------
-- For each test, track:
--   - Conflicts?
--   - How much (virual) bandwidth is consumed (messages of each kind, messages per instance, etc.)
--   - How many client requests were handled
--   - How many client requests were dropped

data ConsensusStats = ConsensusStats
    { title             :: String
    , instancesSent     :: Int    -- How many total instances get transferred in the messages (rough network bandwidth metric)
    , instancesLearned  :: Int    -- How many instances were ultimately learned (measure of productivity)
--    , instancesKnown    :: Int
    , messagesSent      :: Int    -- Count of total messages sent (rough network congestion metric)
    , messagesLost      :: Int    -- Count of total messages lost
    , requestsSubmitted :: Int    -- Count of client requests submitted
    , noOpsLearned      :: Int    -- Compare instancesLearned - noOpsLearned to see how many requests (of requestsSubmitted) were learned.
--    , roundsPrepared    :: Set Round -- Total number of unique leadership changes
--    , serialLogLength   :: Int
    , memoryResidence   :: Int    -- Max # instances residing in memory at one time...
--    , maxMessageSize    :: Int    -- Max instances in a single message?
    , prepared          :: Int    -- Leadership changes
    } deriving (Show)

newStats = ConsensusStats
    { title             = "Untitled"
    , instancesSent     = 0
--    , instancesKnown    = 0
    , instancesLearned  = 0
    , messagesSent      = 0
    , requestsSubmitted = 0
    , noOpsLearned      = 0
--    , roundsPrepared    = Set.empty
--    , serialLogLength   = 0
    , messagesLost      = 0
    , memoryResidence   = 0
--    , maxMessageSize    = 0
    , prepared          = 0
    }

-------------------------------
broadcast :: From -> [To] -> Maybe a -> [(From,To,a)]
broadcast from to Nothing = []
broadcast from to (Just m) = zip3 (repeat from) to (repeat m)

-------------------------------
a0 = Round 0 NodeA
a1 = Round 1 NodeA
b0 = Round 0 NodeB
b1 = Round 1 NodeB

checkAssumptions =
    if (NodeA  < NodeB)     &&
       (Nobody < NodeA)     &&
       (NodeI  < MaxNodeID) &&
       (List.sort [4,5,1,3,2]) == [1,2,3,4,5] &&
       (a0 == a0) && (a1 == a1) && (b0 == b0) && (b1 == b1) &&
       (a0 /= a1) && (a1 /= b1) && (b0 /= b1) && (b0 /= a0) &&
       (a0 <  a1) && (a0 <  b0) && (a0 <  b1) && (a1 <  b1) && (b0 < b1) &&
       (a1 >  a0) && (a1 >  b0) && (b0 >  a0) && (b1 >  b0) && (b1 > a0)
    then True
    else False

-------------------------------
data Config = Config
      { nodes :: Int
      , killNodes :: [NodeID]
      }


