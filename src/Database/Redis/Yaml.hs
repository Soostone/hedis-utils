{-# LANGUAGE OverloadedStrings #-}
module Database.Redis.Yaml where

-------------------------------------------------------------------------------
import           Data.Yaml
import           Database.Redis as H
import           Network.Socket
-------------------------------------------------------------------------------


instance FromJSON H.ConnectInfo where
    parseJSON (Object v) = do
        host <- v .: "host"
        port <- v .: "port"
        n <- v .: "max-conn"
        to <- v .: "timeout"
        return $ H.defaultConnectInfo
          { H.connectHost = host
          , H.connectPort = H.PortNumber (fromIntegral (port::Int))
          , H.connectMaxConnections = n
          , H.connectMaxIdleTime = realToFrac (to::Double)}


