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
import qualified Data.ByteString.Char8               as B
import           Data.IORef
import           Data.Serialize                      as S
import           Data.Typeable
import           Database.Redis
import           Database.Redis.Utils
import           GHC.Generics
import           Test.Framework
import           Test.Framework.Providers.SmallCheck
import           Test.SmallCheck
import           Test.SmallCheck.Series
-------------------------------------------------------------------------------





main = do
    c <- connect defaultConnectInfo
    defaultMain [
         testProperty "simple BS roundtrip" (prop_roundtrip c)
       , testProperty "push/pop FIFO" (prop_fifo c)
       , testProperty "serialize custom type works" prop_serialize_works
       , testProperty "push/pop FIFO custom type" (prop_fifo_custom c)
       , testProperty "locking works" (prop_locking c)
       , testProperty "renewable locks work" (prop_renewable_lock c)
       , testProperty "short blockLock fails" (prop_blocklock_fail c)
       , testProperty "blockLock eventually succeeds" (prop_blocklock c)
       ]


-------------------------------------------------------------------------------
prop_locking :: Connection -> (B.ByteString, B.ByteString) -> Property IO
prop_locking c (ns, nm) = monadic $ do
  l <- runRedis c $ acquireLock ns 3 nm
  l' <- runRedis c $ acquireLock ns 3 nm
  runRedis c $ releaseLock ns nm
  return $ l && not l'


-------------------------------------------------------------------------------
prop_blocklock :: Connection -> (B.ByteString, B.ByteString) -> Property IO
prop_blocklock c (ns, nm) = monadic $ do
      l <- runRedis c $ blockLock redisPolicy ns 0.5 nm
      l' <- runRedis c $ blockLock redisPolicy ns 0.5 nm
      runRedis c $ releaseLock ns nm
      return $ l && l'
  where
    redisPolicy = capDelay 5000000 $ exponentialBackoff 25000 <> limitRetries 12


-------------------------------------------------------------------------------
prop_blocklock_fail c (ns, nm) = monadic $ do
    l <- runRedis c $ blockLock redisPolicy ns 3 nm
    l' <- runRedis c $ blockLock redisPolicy ns 3 nm
    runRedis c $ releaseLock ns nm
    return $ l && not l'
  where
    redisPolicy = constantDelay 500 <> limitRetries 2


------------------------------------------------------------------------------
prop_renewable_lock c = monadic $ do
    let ns = "locktest"
        nm = "renewable"
    let lockTime = 2
    l <- runRedis c $ acquireRenewableLock ns lockTime nm
    a <- spawnLockRenewer c ns lockTime nm
    stoleRef <- newIORef False
    let tryToSteal = do
          stole <- runRedis c $ acquireRenewableLock ns lockTime nm
          if stole
            then putStrLn "Steal!" >> writeIORef stoleRef True
            else tryToSteal
    b <- async tryToSteal
    threadDelay (round $ 15 * 1e6)
    cancel b
    threadDelay (round $ 1 * 1e6)
    cancel a
    stole <- readIORef stoleRef
    return $ l && not stole


------------------------------------------------------------------------------
spawnLockRenewer
    :: Connection
    -> B.ByteString
    -> Double
    -> B.ByteString
    -> IO (Async ())
spawnLockRenewer c lock to nm = do
    liftIO $ asyncLinked $ forever $ do
        let sleepTime = if to > 4 then to - 2 else to / 2
        threadDelay (round $ sleepTime * 1e6)
        go
  where
    go = do
      putStrLn "Calling renew"
      res <- runRedis c $ renewRenewableLock lock to nm
      if res
        then putStrLn "Renew succeeded"
        else putStrLn "Renew failed" >> go



data InternalCancel = InternalCancel deriving (Eq,Show,Read,Ord,Typeable)
instance Exception InternalCancel

------------------------------------------------------------------------------
asyncLinked :: IO () -> IO (Async ())
asyncLinked f = do
    a <- async (f `catch` (\ InternalCancel -> return ()))
    link a
    return a

-------------------------------------------------------------------------------
-- | Cancel a linked action without killing its supervising thread.
cancelLinked :: Async a -> IO ()
cancelLinked a = cancelWith a InternalCancel


-------------------------------------------------------------------------------
prop_roundtrip :: Connection -> B.ByteString -> Property IO
prop_roundtrip c x = monadic $ runRedis c $ do
    lpush "testing" [x]
    res <- unwrap $ lpop "testing"
    return $ res == Just x


prop_fifo :: Connection -> Int -> String -> Property IO
prop_fifo c x y = monadic $ runRedis c $ do
    pushFIFO "testing" (x,y)
    [(a,b)] <- popFIFO "testing" 1
    return $ a == x && b == y



data TestData = TestData {
      tdInt :: Int
    , tdBS  :: B.ByteString
    } deriving (Eq, Generic, Show, Read)


instance Serialize TestData
instance Monad m => Serial m TestData


instance Monad m => Serial m B.ByteString where
    series = B.pack `fmap` series


prop_serialize_works :: TestData -> Bool
prop_serialize_works x = S.decode (S.encode x) == Right x

prop_fifo_custom :: Connection -> TestData -> Property IO
prop_fifo_custom c x = monadic $ runRedis c $ do
    pushFIFO "testing_custom" x
    [n] <- popFIFO "testing_custom" 1
    return $ n == x

