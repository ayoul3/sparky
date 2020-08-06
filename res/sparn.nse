local stdnse = require "stdnse"
local shortport = require "shortport"
local table = require "table"

description = [[
Identifies a Spark cluster manager running in Standalone mode.
]]

---
-- @usage
-- nmap --script spark-info <host>
--
-- @output
-- PORT     STATE SERVICE
-- 7077/tcp open  Apache Spark
-- | spark:
-- |_  Authentication : Not required
--
-- @changelog
-- 2020-07-01 - v0.1 - created by Ayoul3
--
-- Tested on Spark 2.3.x, 2.4.x and 3.0.0

author = "Ayoul3"
license = "Same as Nmap--See https://nmap.org/book/man-legal.html"
categories = {"safe", "discovery"}

portrule = shortport.portnumber({7077})

-- nonce with two magic bytes 197 03. 8 random bytes and the size of the next payload in the last four bytes:
local hello_nonce = "\00\00\00\00\00\00\00\197\03\115\112\97\114\107\97\97\97\00\00\00\176"

-- serialized class org.apache.spark.rpc.netty.RPCEndpointVerifier$CheckExistence
local hello_msg = "\01\00\12\49\57\50\46\49\54\56\46\49\46\50\49\00\00\231\68\01\00\12\49\57\50\46\49\54\56\46\49\46\50\56\00\00\27\165\00\17\101\110\100\112\111\105\110\116\45\118\101\114\105\102\105\101\114\172\237\00\05\115\114\00\61\111\114\103\46\97\112\97\99\104\101\46\115\112\97\114\107\46\114\112\99\46\110\101\116\116\121\46\82\112\99\69\110\100\112\111\105\110\116\86\101\114\105\102\105\101\114\36\67\104\101\99\107\69\120\105\115\116\101\110\99\101\108\25\30\174\142\64\192\31\02\00\01\76\00\04\110\97\109\101\116\00\18\76\106\97\118\97\47\108\97\110\103\47\83\116\114\105\110\103\59\120\112\116\00\06\77\97\115\116\101\114"

action = function(host, port)
  local out =  {}
  local authentication = "unknown"
  local client_ident = nmap.new_socket()
  local catch = function()
    client_ident:close()
  end
  local try = nmap.new_try(catch)
  try(client_ident:connect(host.ip, port))
  try(client_ident:send(hello_nonce))
  try(client_ident:send(hello_msg))

  local resp_nonce = try(client_ident:receive_bytes(68))
  if string.sub(resp_nonce, 10, 14) ~= "spark" then
    return nil
  end
  port.version.name = "Apache Spark"
  port.version.product = "Apache Spark"
  nmap.set_port_version(host, port, "hardmatched")
  if resp_nonce:find("Expected SaslMessage") then
    authentication = "SASL"
  elseif resp_nonce:find("valuexp") then
    authentication = "Not required"
  end
  table.insert(out, string.format("Authentication : %s", authentication))
  return stdnse.format_output(true, out)
end