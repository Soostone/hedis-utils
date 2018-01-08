{-# LANGUAGE OverloadedStrings #-}

module Database.Redis.Configurator
    ( getRedisConfig
    ) where


-------------------------------------------------------------------------------
import           Control.Applicative     as A
import qualified Data.Configurator       as C
import qualified Data.Configurator.Types as C
import           Database.Redis          as H
-------------------------------------------------------------------------------



-------------------------------------------------------------------------------
getRedisConfig :: C.Config -> IO H.ConnectInfo
getRedisConfig c = do
    host <- C.lookupDefault "localhost" c "host"
    p <- C.lookupDefault 6379 c "port"
    let port = H.PortNumber . fromIntegral $ (p :: Int)
    n <- C.lookupDefault 3 c "max-conn"
    to <- fromIntegral A.<$> C.lookupDefault (15 :: Int) c "timeout"
    return $ H.defaultConnectInfo { H.connectHost = host
                                  , H.connectPort = port
                                  , H.connectMaxConnections = n
                                  , H.connectMaxIdleTime = to }
