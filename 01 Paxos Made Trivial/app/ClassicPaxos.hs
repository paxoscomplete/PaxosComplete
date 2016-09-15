module ClassicPaxos where

import Data.Set as Set
import Data.Map as Map
import Data.IntMap as IntMap
import Control.Monad.State
import Paxos

-------------------------------
-- Classic Paxos
-------------------------------
data Message 
    = Timeout
    | Request  { requestValue :: Value }
    | Prepare  { prepareRound :: Round }
    | Promise  { promiseRound :: Round, lastVote    :: Vote }
    | Propose  {                        proposeVote :: Vote }
    | Accepted {                        acceptVote  :: Vote }
    deriving (Eq, Show)

data Node = Leader   { preparedRound :: Round, promiseQuorum :: Set NodeID, highestVote :: Vote }
          | Acceptor { promisedRound :: Round,                              vote        :: Vote }
          | Learner  {                         learnQuorum   :: Set NodeID, learnedVote :: Vote } deriving (Show)

data Group = Group { quorumNeeded :: Int } deriving (Show)

-------------------------------
leader :: From -> To -> Message -> Group -> State Node (Maybe Message)
leader _ _ Timeout _ = do
    Leader preparedRound _ _ <- get                           -- Load current Leader state
    return . Just $ Prepare  { prepareRound = preparedRound } -- Send new Prepare message

leader (From from) (To myID) (Promise promise lastVote) _ = do
    Leader preparedRound promiseQuorum highestVote <- get
    -- Update Leader's choice of value
    -- modify (\a -> a { highestVote = chooseHighest lastVote (highestVote ld) })  -- TODO: This is an optimization; leader learns newer proposed values, even if accpetor has already promised to a higher round..
    -- Update Leader's promise quorum
    case compare promise preparedRound of
      LT -> return ()
      EQ -> modify (\a -> a { promiseQuorum = Set.insert from promiseQuorum
                            , highestVote   = chooseHighest lastVote highestVote })
      GT -> modify (\a -> a { promiseQuorum = Set.empty                   -- Optimization: Another leader detected, clear quorum so we won't try to propose
                            , preparedRound = promoteRound promise myID   -- Optimization: Implicit NAK - jump to higher round number
                            , highestVote   = chooseHighest lastVote highestVote })

    -- if promise == (preparedRound ld)
    -- then modify (\a -> a { promiseQuorum = Set.insert from (promiseQuorum ld)
    --                      , highestVote   = chooseHighest lastVote (highestVote ld) })
    -- else return ()
    return Nothing

leader _ _ (Request value) group = do
    Leader preparedRound promiseQuorum highestVote <- get
    let proposeVote = chooseProposal preparedRound highestVote value

    if Set.size promiseQuorum < quorumNeeded group  -- Are we leading?
    then return Nothing
    else do
        modify (\a -> a         { highestVote = proposeVote })
        return . Just $ Propose { proposeVote = proposeVote }

leader _ _ _ _ = return Nothing

-------------------------------
acceptor :: From -> To -> Message -> Group -> State Node (Maybe Message)
acceptor _ _ (Prepare prepRound) _ = do
    Acceptor promisedRound vote  <- get
    let newRound = max prepRound promisedRound

    modify (\a -> a         { promisedRound = newRound })
    return . Just $ Promise { promiseRound  = newRound
                            , lastVote      = vote }

acceptor _ _ (Propose proposedVote) _ = do
    Acceptor promisedRound vote  <- get
    let Vote proposedRound proposedValue = proposedVote
    let newVote = chooseHighest vote proposedVote

    if proposedRound < promisedRound
    then return Nothing
    else do
        modify (\a -> a          { promisedRound = proposedRound  --max  promisedRound (votedRound <= proposedRound)
                                 , vote          = newVote })
        return . Just $ Accepted { acceptVote    = newVote }

acceptor _ _ _ _ = return Nothing

-------------------------------
learner :: From -> To -> Message -> Group -> State Node (Maybe Message)
learner (From from) _ (Accepted acceptedVote) group = do
    Learner learnQuorum learnedVote <- get
    let Vote acceptedRound _ = acceptedVote
    let Vote learnedRound  _ = learnedVote

    if acceptedVote == learnedVote
    then modify (\a -> a { learnQuorum = Set.insert from learnQuorum })
    else do
      case compare acceptedRound learnedRound of
          LT -> return ()
          _  -> modify (\a -> a { learnQuorum = Set.singleton from
                                , learnedVote = acceptedVote })
    return Nothing

learner _ _ _ _ = return Nothing




-----------------------------
------ TEST INTERFACE -------
-----------------------------

-- How to initialize each of the Roles
makeLeader   a = Leader   { preparedRound = Round 0 a, promiseQuorum = Set.empty, highestVote = blank }
makeAcceptor a = Acceptor { promisedRound = zero,                                 vote        = blank }
makeLearner  a = Learner  {                            learnQuorum   = Set.empty, learnedVote = blank }
makeGroup    n = Group    { quorumNeeded = (n `div` 2) + 1 }

-- Use configuration to build a network for testing
makeNodes numLeaders numAcceptors numLearners = 
  let group     = makeGroup numAcceptors
      leaders   = Prelude.map (\i -> (i, makeLeader   i)) (take numLeaders   leaderIDs)
      acceptors = Prelude.map (\i -> (i, makeAcceptor i)) (take numAcceptors acceptorIDs)
      learners  = Prelude.map (\i -> (i, makeLearner  i)) (take numLearners  learnerIDs)
  in Prelude.foldl (\m (i,n) -> Map.insert i n m) Map.empty $ leaders ++ acceptors ++ learners

-- How to deliver messages to each Role
deliverToNode :: From -> To -> Message -> Group -> Node -> (Maybe Message,Node)
deliverToNode f t m g n@(Leader   _ _ _) = runState (leader   f t m g) n
deliverToNode f t m g n@(Acceptor _ _)   = runState (acceptor f t m g) n
deliverToNode f t m g n@(Learner  _ _)   = runState (learner  f t m g) n

learnerValues :: Group -> Node -> IntMap Value
learnerValues group (Learner learnQuorum (Vote _ v)) = if Set.size learnQuorum >= quorumNeeded group then IntMap.singleton 0 v else IntMap.empty
learnerValues _ _ = IntMap.empty

lowestUnstableInstance group (Learner learnQuorum _) = if Set.size learnQuorum >= quorumNeeded group then 1 else 0
lowestUnstableInstance _ _ = 9999999999

countStats stats Timeout            = stats { title          = "ClassicPaxos     " }
countStats stats (Request _)        = stats { requestsSubmitted = 1 + (requestsSubmitted stats)
                                            , messagesSent   = 1 + (messagesSent stats) }
countStats stats (Prepare r)        = stats { prepared       = (prepared stats) + 1
                                            , messagesSent   = 1 + (messagesSent stats) }
countStats stats (Promise _ _)      = stats { instancesSent  = 1 + (instancesSent stats)
                                            , messagesSent   = 1 + (messagesSent stats) }
countStats stats (Propose _)        = stats { instancesSent  = 1 + (instancesSent stats)
                                            , messagesSent   = 1 + (messagesSent stats) }
countStats stats (Accepted _)       = stats { instancesSent  = 1 + (instancesSent stats)
                                            , messagesSent   = 1 + (messagesSent stats) }

-----------------------------
-------- SIMULATION ---------
-----------------------------

type Network = ([(From,To,Message)], Map NodeID Node)

stabilize = [Tick NodeA, Tick NodeA, Req NodeA ValueA]

-------------------------------
nodeIDs :: Map NodeID a -> [To]
nodeIDs = Prelude.map To . Map.keys

networkDeliver (From from) (To to) msg msgs group net =
    case Map.lookup to net of
      Nothing   -> error $ "NodeID not found in network! " -- ++ (show to) ++ " not in " ++ (show $ Map.keys net)
      Just node -> let (msg',node') = deliverToNode (From from) (To to) msg group node
                   in (msgs ++ (broadcast (From to) (nodeIDs net) msg'), Map.insert to node' net)

-------------------------------
handleEvent :: Group -> Network -> Event -> Network
handleEvent group (msgs, net)               (Tick to)    = networkDeliver (From to) (To to) Timeout msgs group net
handleEvent group (msgs, net)               (Req to val) = networkDeliver (From to) (To to) (Request val) msgs group net
handleEvent group ([], net)                 _            = ([],net)
handleEvent group (((From from),(To to),msg):msgs, net) _ | from == to = networkDeliver (From from) (To to) msg msgs group net  -- Always deliver messages to ourself
handleEvent group ((from,to,msg):msgs, net) Deliver      = networkDeliver from to msg msgs group net
handleEvent group (msg:msgs, net)           Drop         = (msgs, net)
handleEvent group (m:ms, net)               Duplicate    = (m:m:ms, net)
handleEvent group (m:ms, net)               Shift        = (ms ++ [m], net)
--handleEvent group (msgs, net)               e            = error $ "Unhandled Event! " ++ (show e)

-------------------------------
runNetwork :: Group -> Network -> [Event] -> Network
runNetwork group net events = Prelude.foldl (handleEvent group) net events

stableEvents :: Group -> Network -> [Event] -> Network
stableEvents group net@([], _) []     = net
stableEvents group net@([], _) (e:es) = stableEvents group (handleEvent group net e) es
stableEvents group net         es     = stableEvents group (handleEvent group net Deliver) es

checkConsistency :: IntMap Value -> IntMap Value -> IntMap Value
checkConsistency = IntMap.unionWith (\a b -> if a /= b then error "INCONSISTENCY" else a)

extractStableValues :: Group -> Map NodeID Node -> IntMap Value
extractStableValues group = Map.foldl (\a -> checkConsistency a . learnerValues group) IntMap.empty

prop_ConsensusAchieved :: Group -> Network -> [Event] -> Bool
prop_ConsensusAchieved group network events =
  let unstable    = runNetwork group network $ (Tick NodeA):events  -- First event is always Tick
      (_,stable)  = stableEvents group unstable stabilize
      allVals     = extractStableValues group stable  --allNodeVals stable
  in Prelude.notElem NoValue (IntMap.elems allVals)

countEvent ((from,to,m):ms, net) Deliver      stats = countStats stats m
countEvent (msgs, net)           (Tick to)    stats = countStats stats Timeout
countEvent (msgs, net)           (Req to val) stats = countStats stats (Request val)
countEvent ((from,to,m):ms, net) Drop         stats = countStats stats m
countEvent _                     _            stats = stats

handleEventWithStats :: Group -> (Network, ConsensusStats) -> Event -> (Network, ConsensusStats)
handleEventWithStats group = (\(n,s) e -> (handleEvent group n e, countEvent n e s))

runWithStats :: Group -> Network -> ConsensusStats -> [Event] -> (Network, ConsensusStats)
runWithStats group net stats events = Prelude.foldl (handleEventWithStats group) (net,stats) events

stableWithStats :: Group -> (Network, ConsensusStats) -> [Event] -> (Network, ConsensusStats)
stableWithStats group net@(([], _), _) []     = net
stableWithStats group net@(([], _), _) (e:es) = stableWithStats group (handleEventWithStats group net e) es
stableWithStats group net              es     = stableWithStats group (handleEventWithStats group net Deliver) es

collectStableStats :: Group -> Network -> [Event] -> (Map NodeID Node, ConsensusStats)
collectStableStats group network events =
  let ((_,nodes),stats)  = stableWithStats group (network, newStats) events 
      allVals      = extractStableValues group nodes
      stats'       = stats { instancesLearned = IntMap.size allVals
                           , noOpsLearned     = IntMap.size (IntMap.filter (\v -> v == NoOp || v == NoValue) allVals)
                           --, serialLogLength  = Prelude.foldl (\a b -> min a b) 99999999 . Prelude.map (lowestUnstableInstance group) $ Map.elems nodes
                           }
  in (nodes,stats')

collectStats :: Group -> Network -> [Event] -> (Map NodeID Node, ConsensusStats)
collectStats group network events =
  let (unstable,stats) = runWithStats group network newStats $ (Tick NodeA):events  -- First event is always Tick
      (_,stable)       = stableEvents group unstable stabilize
      allVals          = extractStableValues group stable
      stats'           = stats { instancesLearned = IntMap.size allVals
                               , noOpsLearned     = IntMap.size (IntMap.filter (\v -> v == NoOp || v == NoValue) allVals)
                               --, serialLogLength  = Prelude.foldl (\a b -> min a b) 99999999 . Prelude.map (lowestUnstableInstance group) $ Map.elems stable
                               }
  in (stable, stats')

-----------------------
makeNetwork c = let n = nodes c in ([], makeNodes n n n)

runNetworkClassicPaxos              :: Config -> [Event] -> String
runNetworkClassicPaxos c e = show $ runNetwork (makeGroup $ nodes c) (makeNetwork c) e

stableEventsClassicPaxos            :: Config -> [Event] -> String
stableEventsClassicPaxos c e = show $ stableEvents (makeGroup $ nodes c) (makeNetwork c) e

prop_ConsensusAchievedClassicPaxos  :: Config -> [Event] -> Bool
prop_ConsensusAchievedClassicPaxos c = prop_ConsensusAchieved (makeGroup $ nodes c) (makeNetwork c)

collectStableStatsClassicPaxos      :: Config -> [Event] -> ConsensusStats
collectStableStatsClassicPaxos c e = snd $ collectStableStats (makeGroup $ nodes c) (makeNetwork c) e

collectStatsClassicPaxos            :: Config -> [Event] -> ConsensusStats
collectStatsClassicPaxos c e = snd $ collectStats (makeGroup $ nodes c) (makeNetwork c) e


