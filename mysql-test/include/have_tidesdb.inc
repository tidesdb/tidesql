if (`SELECT COUNT(*) = 0 FROM INFORMATION_SCHEMA.ENGINES WHERE engine = 'TidesDB' AND support IN ('YES', 'DEFAULT', 'ENABLED')`)
{
  --skip Test requires TidesDB.
}
