{
  global:
    # Only expose the RedisModule APIs
    RedisModule_OnLoad;
    RedisModule_OnUnload;
  local: *;  # hide everything else
};