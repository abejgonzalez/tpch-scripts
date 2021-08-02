BEGIN {
  regex="[^/]+.tbl"
}
{
  if (match($2, regex)) {
    table=substr($2, RSTART, RLENGTH-4);
    print "ALTER TABLE IF EXISTS " table " RENAME TO " table "_NUMBER;"
  }
}
