
+ ElasticSearch upload for CMS and HTCondor data

This package contains a set of scripts that assist in uploading data from
a HTCondor pool to ElasticSearch.

The majority of the logic is in the conversion of ClassAds to values that
are immediately useful in Kibana / ElasticSearch.  A significant portion
of the logic is very specific to the CMS global pool - but ultimately could
be reused for other HTCondor pools.

