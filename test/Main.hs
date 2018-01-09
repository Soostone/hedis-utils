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
import           Test.Tasty
import           Test.Tasty.Hedgehog
import           Test.Tasty.HUnit
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
  ]

-------------------------------------------------------------------------------
prop_locking :: Connection -> TestTree
prop_locking c = testProperty "locking works" $ property $ do
  ns <- forAll (Gen.bytes (Range.linear 1 20))
  nm <- forAll (Gen.bytes (Range.linear 1 20))
  l <- liftIO $ runRedis c $ acquireLock ns 3 nm
  l' <- liftIO $ runRedis c $ acquireLock ns 3 nm
  liftIO $ runRedis c $ releaseLock ns nm
  HH.assert $ l && not l'


-------------------------------------------------------------------------------
prop_blocklock :: Connection -> TestTree
prop_blocklock c = testProperty "blockLock eventually succeeds" $ property $ do
  l <- liftIO $ runRedis c $ blockLock redisPolicy ns 0.5 nm
  l' <- liftIO $ runRedis c $ blockLock redisPolicy ns 0.5 nm
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
    l <- liftIO $ runRedis c $ blockLock redisPolicy ns 3 nm
    l' <- liftIO $ runRedis c $ blockLock redisPolicy ns 3 nm
    liftIO $ runRedis c $ releaseLock ns nm
    HH.assert $ l && not l'
  where
    redisPolicy :: RetryPolicy
    redisPolicy = constantDelay 500 <> limitRetries 2


------------------------------------------------------------------------------
prop_renewable_lock :: Connection -> TestTree
prop_renewable_lock c = testCase "renewable lock works" $ do
  gotInitialLock <- runRedis c $ acquireRenewableLock ns lockTime nm
  if gotInitialLock
     then do
       a <- spawnLockRenewer c ns lockTime nm
       stoleRef <- newIORef False
       let tryToSteal = do
             stole <- runRedis c $ acquireRenewableLock ns lockTime nm
             if stole
               then putStrLn "Steal!" >> writeIORef stoleRef True
               else tryToSteal
       b <- async tryToSteal
       threadDelay (secondsInMicros 15)
       cancel b
       threadDelay ((secondsInMicros 1))
       cancelLinked a
       stole <- readIORef stoleRef
       assertBool "Expected lock to not be stolen but it was" (not stole)
     else assertFailure "Failed to acquire initial lock"
  where
    ns = "locktest"
    nm = "renewable"
    -- Note: renewable lock only really allows the lock holder to
    -- renew properly if the lock time is > 5s. Renewable locks
    -- acquire an outer lock and then do operations against the target
    -- lock in a blocking fashion. That blocking is hardcoded to 5s,
    -- so it could lock out renew attempts longer than the lock is
    -- alive and steal the lock
    lockTime = 10


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
      putStrLn "Calling renew"
      res <- runRedis c $ renewRenewableLock lock tout nm
      if res
        then putStrLn "Renew succeeded"
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



secondsInMicros :: Double -> Int
secondsInMicros = round . (* 1e6)
