{-# LANGUAGE FlexibleInstances, MultiParamTypeClasses #-}

-- stack build --executable-profiling --library-profiling --ghc-options="-fprof-auto -rtsopts -O3"
-- stack build --ghc-options=-O3

module Main where

import Data.Set as Set
import Data.Map as Map
import Data.IntMap as IntMap
import Test.QuickCheck
import Test.SmallCheck
import Test.SmallCheck.Series
import Test.SmallCheck.Drivers
import Control.Monad.State
import Debug.Trace

import Paxos
import ClassicPaxos

listLongerThan :: Int -> Gen [Event]
listLongerThan x = replicateM (x+1) arbitrary

testConfig3 = Config
      { nodes = 3
      , killNodes = []
      }

testConfig3WithDeadNode = Config
      { nodes = 3
      , killNodes = [NodeE]   -- Kill the second Acceptor
      }

testConfig5 = Config
      { nodes = 5
      , killNodes = []
      }

-------------------------------
main = do
      if checkAssumptions
      then return ()
      else error "Assumptions FAILED!"

      putStrLn "---------------------------------"
      let collectStats = [ collectStatsClassicPaxos ]
      let collectStableStats = [ collectStableStatsClassicPaxos ]
      let prop_ConsensusAchieved = [ prop_ConsensusAchievedClassicPaxos ]
      let collectStatsLong = [ collectStatsClassicPaxos ]

      putStrLn "---------------------------------"
      putStrLn "QuickCheck"
      mapM (quickCheckWith stdArgs { maxSuccess = 1000 }) (Prelude.map (\f -> f testConfig3) prop_ConsensusAchieved)

      putStrLn "---------------------------------"
      putStrLn "Smoke Test: 100 instances, 3 nodes, multi-master (should learn 1 instance, 0 NoOps)"
      mapM (\f -> putStrLn . show . f testConfig3 $ concat . replicate 100 $ [ Req NodeA ValueA, Req NodeB ValueB, Req NodeC ValueC, Tick NodeA, Tick NodeB, Tick NodeC ]) collectStableStats

      putStrLn "---------------------------------"
      putStrLn "Smoke Test with 1 dead node"
      mapM (\f -> putStrLn . show . f testConfig3WithDeadNode $ concat . replicate 100 $ [ Req NodeA ValueA, Req NodeB ValueB, Req NodeC ValueC, Tick NodeA, Tick NodeB, Tick NodeC ]) collectStableStats

      putStrLn "---------------------------------"
      putStrLn "Long Random Tests"
      (a:b:c:_) <- sample' . listLongerThan $ 1000
      mapM (\f -> putStrLn . show $ f testConfig3 a) collectStatsLong

      putStrLn "---------------------------------"
      putStrLn "smallCheck -- 6"
      smallCheck 6 (prop_ConsensusAchievedClassicPaxos testConfig3)
      putStrLn "smallCheck -- 7"
      smallCheck 7 (prop_ConsensusAchievedClassicPaxos testConfig3)









