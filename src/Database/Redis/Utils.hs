{-# LANGUAGE OverloadedStrings #-}

module Database.Redis.Utils
    (

     -- * General Utilities
      expect
    , unexpected
    , retryRedis
    , runRedis'

     -- * Locking
    , blockLock
    , acquireLock
    , releaseLock
    , mkLockName
    , mkTimeOut

    ) where


-------------------------------------------------------------------------------
import           Control.Exception
import           Control.Monad
import           Control.Monad.IO.Class
import           Control.Retry
import           Data.ByteString.Char8  (ByteString)
import qualified Data.ByteString.Char8  as B
import           Data.Default
import           Data.Time.Clock.POSIX
import           Database.Redis         hiding (decode)
import qualified Database.Redis         as R
import           Prelude                hiding (catch)
import           Safe
-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
-- | Short-hand for running redis and retrying a few times.
runRedis' :: Connection -> Redis a -> IO a
runRedis' = retryRedis 3 "Generic retryRedis"


-------------------------------------------------------------------------------
-- | Retry a given redis action multiple times if it fails.
retryRedis
    :: Int
    -- ^ Number of retries
    -> String
    -- ^ Additional message to display
    -> Connection
    -- ^ Connection pool
    -> Redis a
    -- ^ Action to run
    -> IO a
retryRedis max msg c f = go `catch` handle
  where
    handle :: SomeException -> IO a
    handle e = error . concat $
           [ "Hedis: Retried ", show max, " times but failed. Error: "
           , show e, ". Message: ", msg ]
    go = recoverAll (def { numRetries = limitedRetries max }) $ runRedis c f




-------------------------------------------------------------------------------
-- | Unwrap just enough to know if there was an exception. Expect that
-- there isn't one, getting rid of Either return type from hedis.
expect :: (Monad m, Show a) => m (Either a b) -> m b
expect f = do
  res <- f
  case res of
    Left e -> unexpected e
    Right xs -> return xs


-------------------------------------------------------------------------------
-- | Raise the unexpected exception
unexpected :: Show a => a -> t
unexpected r = error $ "Received an unexpected Left response from Redis. Reply: " ++ show r






                                 -------------
                                 -- Locking --
                                 -------------


-------------------------------------------------------------------------------
-- | Block until lock can be acquired. Will try locking for up to 8
-- times, with a base delay of 25 miliseconds. Exponential backoff
-- from there.
blockLock
    :: RetrySettings
    -- ^ Retry settings for while trying to acquire the lock. As an
    -- example, a 25 milisecond base with 10 exp backoff retries would
    -- work up to a 25 second retry.
    -> B.ByteString
    -- ^ namespace for this lock
    -> Double
    -- ^ timeout in seconds.
    -> B.ByteString
    -- ^ Name of item to lock.
    -> Redis Bool
blockLock set lock to nm = retrying set id $ acquireLock lock to nm



-------------------------------------------------------------------------------
-- | Try to acquire lock in a given namespace. Immediately returns the result.
acquireLock
    :: B.ByteString
    -- ^ namespace for this lock
    -> Double
    -- ^ timeout in seconds
    -> B.ByteString
    -- ^ Name to lock
    -> Redis Bool
acquireLock lock to nm = do
    tm <- mkTimeOut to
    res <- expect $ setnx nm' tm
    case res of
      True -> return True
      False -> do
          curLock <- expect $ R.get nm'
          case curLock of
            -- someone else unlocked it, retry the process
            Nothing -> acquireLock lock to nm
            Just curVal ->
              case readMay $ B.unpack curVal of
                -- there is a value in there but I can't even read it
                Nothing -> getsetMech =<< liftIO getTime
                Just oldTo -> do
                  now <- liftIO getTime
                  case now > oldTo of
                    -- expired timeout, use getset mechanism
                    True -> getsetMech now
                    -- someone has it locked
                    False -> return False
    where
      nm' = mkLockName lock nm

      -- this is a reliable mechanism to override an
      -- old/expired/garbled value in the lock. Prevents race
      -- conditions.
      getsetMech now = do
          e <- expect $ getset nm' (B.pack . show $ now + to)
          case e of
            -- no old value in there, shouldn't happen in practice
            Nothing -> return True
            -- check that value read is expired
            Just curVal' ->
              case readMay $ B.unpack curVal' of
                -- can't even read, bizarre
                Nothing -> return True
                 -- see if it was expired; if not,
                 -- someone else beat us to the lock
                Just oldTo' -> return $ now > oldTo'




-------------------------------------------------------------------------------
-- | Release a lock
releaseLock
    ::  B.ByteString
    -- ^ namespace for this lock
    -> B.ByteString
    -- ^ Name of item to release
    -> Redis Integer
releaseLock lock nm = expect $ del [nm']
    where
      nm' = mkLockName lock nm


-------------------------------------------------------------------------------
mkLockName lock nm = B.intercalate ":" ["_lock", lock, nm]


-------------------------------------------------------------------------------
mkTimeOut :: (MonadIO m) => Double -> m B.ByteString
mkTimeOut to = (B.pack . show . (to +)) `liftM` liftIO getTime



getTime :: IO Double
getTime = realToFrac `fmap` getPOSIXTime
