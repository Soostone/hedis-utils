{-# LANGUAGE DeriveDataTypeable    #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}

module Main where


-------------------------------------------------------------------------------
import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Exception
import           Control.Monad
import           Control.Monad.Trans
import           Control.Retry
import qualified Data.ByteString.Char8    as B
import           Data.IORef
import           Data.Monoid
import           Data.Serialize           as S
import           Data.Typeable
import           Database.Redis
import           Database.Redis.Utils
import           GHC.Generics
import           Hedgehog                 as HH
import qualified Hedgehog.Gen             as Gen
import qualified Hedgehog.Range           as Range
import           System.Timeout
import           Test.Tasty
import           Test.Tasty.Hedgehog
import           Test.Tasty.Options
-------------------------------------------------------------------------------


main :: IO ()
main = do
  c <- connect defaultConnectInfo
  defaultMain (tests c)


-------------------------------------------------------------------------------
tests :: Connection -> TestTree
tests c = testGroup "hedis-utils"
  [ prop_roundtrip c
  , prop_fifo c
  , prop_serialize_works
  , prop_fifo_custom c
  , prop_locking c
  , prop_renewable_lock c
  , prop_blocklock_fail c
  , prop_blocklock c
  , prop_lock_expire c
  , prop_blocklock_exhaustion
  ]


-------------------------------------------------------------------------------
prop_locking :: Connection -> TestTree
prop_locking c = testGroup "locking"
  [ testProperty "works as expected" $ property $ do
      ns <- forAll (Gen.bytes (Range.linear 1 20))
      nm <- forAll (Gen.bytes (Range.linear 1 20))
      l <- liftIO $ runRedis c $ acquireLock ns 3 nm
      l' <- liftIO $ runRedis c $ acquireLock ns 3 nm
      liftIO $ runRedis c $ releaseLock ns nm
      HH.assert $ l && not l'
  , testProperty "returns True even if the timeout is immediate or negative" $ property $ do
      let ns = "locktest"
      let nm = "lock_immediate_timeout"
      tout <- forAll (Gen.double (Range.linearFrac (-100) 0))
      locked <- liftIO $ runRedis c $ acquireLock ns tout nm
      liftIO $ runRedis c $ releaseLock ns nm
      footnote "Expected lock to acquire but it did not."
      HH.assert locked
  ]

-------------------------------------------------------------------------------
prop_lock_expire :: Connection -> TestTree
prop_lock_expire c = unlessOverridden (HedgehogTestLimit 25) $ testProperty "locks expire eventually on their own when not released" $ property $ do
  lockLifespan <- forAll (Gen.double (Range.linearFrac 0.01 1))
  waitFor <- forAll (Gen.double (Range.linearFrac (lockLifespan + 0.1) 1))
  l <- liftIO $ runRedis c $ acquireLock ns lockLifespan nm
  HH.assert l
  liftIO $ threadDelay $ secondsInMicros waitFor
  l' <- liftIO $ runRedis c $ acquireLock ns lockLifespan nm
  liftIO $ runRedis c $ releaseLock ns nm
  footnote "Expected second lock to succeed"
  HH.assert l'
  where
    ns = "locktest"
    nm = "lock_expire"


-------------------------------------------------------------------------------
prop_blocklock :: Connection -> TestTree
prop_blocklock c = unlessOverridden (HedgehogTestLimit 25) $ testProperty "blockLock eventually succeeds" $ property $ do
  lockTTL <- forAll (Gen.double (Range.linearFrac 0.01 0.5))
  l <- liftIO $ blockLock c redisPolicy ns lockTTL nm
  l' <- liftIO $ blockLock c redisPolicy ns lockTTL nm
  liftIO $ runRedis c $ releaseLock ns nm
  HH.assert $ l && l'
  where
    ns = "locktest"
    nm = "blocklock"
    redisPolicy :: RetryPolicy
    redisPolicy = capDelay 5000000 $ exponentialBackoff 25000 <> limitRetries 12


-------------------------------------------------------------------------------
prop_blocklock_fail :: Connection -> TestTree
prop_blocklock_fail c = testProperty "short blocklock fails" $ property $ do
    ns <- forAll (Gen.bytes (Range.linear 1 20))
    nm <- forAll (Gen.bytes (Range.linear 1 20))
    l <- liftIO $ blockLock c redisPolicy ns 3 nm
    l' <- liftIO $ blockLock c redisPolicy ns 3 nm
    liftIO $ runRedis c $ releaseLock ns nm
    HH.assert $ l && not l'
  where
    redisPolicy :: RetryPolicy
    redisPolicy = constantDelay 500 <> limitRetries 2


------------------------------------------------------------------------------
prop_renewable_lock :: Connection -> TestTree
prop_renewable_lock c = unlessOverridden (HedgehogTestLimit 25) $ testProperty "renewable lock works" $ property $ do
  lockTime <- forAll (Gen.double (Range.linearFrac 0.01 1))
  gotInitialLock <- liftIO $ runRedis c $ acquireLock ns lockTime nm
  when (not gotInitialLock) $ footnote "Failed to acquire initial lock"
  HH.assert gotInitialLock
  stoleRef <- liftIO $ newIORef False
  liftIO $ do
    renewer <- liftIO $ spawnLockRenewer c ns lockTime nm
    let tryToSteal = do
          stole <- runRedis c $ acquireLock ns lockTime nm
          if stole
            then putStrLn "Steal!" >> writeIORef stoleRef True
            else do
              threadDelay (secondsInMicros (lockTime / 2))
              tryToSteal
    thief <- liftIO $ async tryToSteal
    let thiefLife = lockTime * 3
    threadDelay (secondsInMicros thiefLife)
    cancel thief
    cancelLinked renewer
    runRedis c $ releaseLock ns nm
  stole <- liftIO $ readIORef stoleRef
  when stole $ footnote "Expected lock to not be stolen but it was"
  HH.assert (not stole)
  where
    ns = "locktest"
    nm = "renewable"


------------------------------------------------------------------------------
spawnLockRenewer
    :: Connection
    -> B.ByteString
    -> Double
    -> B.ByteString
    -> IO (Async ())
spawnLockRenewer c lock tout nm = do
    liftIO $ asyncLinked $ forever $ do
        let sleepTime = if tout > 4 then tout - 2 else tout / 2
        threadDelay (secondsInMicros sleepTime)
        go
  where
    go = do
      res <- runRedis c $ renewRenewableLock lock tout nm
      if res
        then return ()
        else putStrLn "Renew failed" >> go



data InternalCancel = InternalCancel deriving (Eq,Show,Read,Ord,Typeable)
instance Exception InternalCancel

------------------------------------------------------------------------------
asyncLinked :: IO () -> IO (Async ())
asyncLinked f = do
    a <- async (f `catch` (\InternalCancel -> return ()))
    link a
    return a

-------------------------------------------------------------------------------
-- | Cancel a linked action without killing its supervising thread.
cancelLinked :: Async a -> IO ()
cancelLinked a = cancelWith a InternalCancel


-------------------------------------------------------------------------------
prop_roundtrip :: Connection -> TestTree
prop_roundtrip c = testProperty "simple BS roundtrip" $ property $ do
    x <- forAll (Gen.bytes (Range.linear 0 20))
    res <- liftIO $ runRedis c $ do
      _ <- lpush "testing" [x]
      unwrap $ lpop "testing"
    res === Just x


prop_fifo :: Connection -> TestTree
prop_fifo c = testProperty "push/pop FIFO" $ property $ do
    int <- forAll (Gen.int Range.linearBounded)
    str <- forAll (Gen.string (Range.linear 0  20) Gen.unicode)
    [(a,b)] <- liftIO $ runRedis c $ do
      pushFIFO "testing" (int,str)
      popFIFO "testing" 1
    a === int
    b === str



data TestData = TestData {
      tdInt :: Int
    , tdBS  :: B.ByteString
    } deriving (Eq, Generic, Show, Read)


genTestData :: Gen TestData
genTestData = TestData
  <$> Gen.int Range.linearBounded
  <*> Gen.bytes (Range.linear 0 20)


instance Serialize TestData


prop_serialize_works :: TestTree
prop_serialize_works = testProperty "serialize custom type works" $ property $ do
  td <- forAll genTestData
  S.decode (S.encode td) === Right td

prop_fifo_custom :: Connection -> TestTree
prop_fifo_custom c = testProperty "push/pop FIFO custom type" $ property $ do
  td <- forAll genTestData
  [n] <- liftIO $ runRedis c $ do
    pushFIFO "testing_custom" td
    popFIFO "testing_custom" 1
  n === td


-------------------------------------------------------------------------------
prop_blocklock_exhaustion :: TestTree
prop_blocklock_exhaustion = testProperty "blockLock does not hold a connection the entire time" $ property $ do
  c1 <- liftIO $ connect defaultConnectInfo { connectMaxConnections = 1 }
  l1 <- liftIO $ runRedis c1 $ acquireLock ns lockTime nm
  unless l1 $ footnote "Failed to acquire initial lock"
  HH.assert l1

  blockAsync <- liftIO $ async $
    blockLock c1 blockPolicy ns lockTime nm

  stat <- liftIO $ timeout (secondsInMicros 1) $
    runRedis c1 ping

  unless (stat == Just (Right Pong)) $
    footnote "Expected blockLock to not hold connection but it did"
  stat === Just (Right Pong)

  liftIO $ cancel blockAsync
  liftIO $ runRedis c1 $ releaseLock ns nm
  where
    blockPolicy :: RetryPolicy
    blockPolicy = capDelay (secondsInMicros 5) (exponentialBackoff 10000)
    lockTime = 5
    ns = "locktest"
    nm = "blockLock"


-------------------------------------------------------------------------------
secondsInMicros :: Double -> Int
secondsInMicros = round . (* 1e6)


-------------------------------------------------------------------------------
-- | If the current value is the default value, override with the
-- given value, otherwise, respect the user's input
unlessOverridden :: (IsOption v, Eq v) => v -> TestTree -> TestTree
unlessOverridden overrideV = adjustOption go
  where
     go vGiven
       | vGiven == defaultValue = overrideV
       | otherwise              = vGiven
