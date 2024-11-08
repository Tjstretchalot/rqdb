from typing import Literal


ReadConsistency = Literal["none", "weak", "linearizable", "strong"]
"""https://rqlite.io/docs/api/read-consistency/

- **none**: Read the local state of whatever node the request hits. Fastest if you
  can accept the result may not be completely fresh. (E.g., doesn't include the
  last handful of milliseconds of inserts). The only level that will spread out
  read load.

- **weak**: If the targetted node is not the leader, redirect to the leader.
  Otherwise, read the local state of the leader. Fastest if the result must be
  fresh except around leader changes.

- **linearizable**: If the targetted node is not the leader, redirect to the leader.
  Otherwise, verify we were still the leader when we received the request, then
  read the local state. Much slower than weak but doesn't clutter the raft log,
  so much faster than strong. Use if you are purposely racing another request
  and need to get a linearizable answer even if the leader is currently changing.

- **strong**: Run the query through the Raft process. Slow, doesn't give any guarrantees
  not already available from linearizable. Was only occassionally useful before
  linearizable was available and is now almost never useful.
"""

DEFAULT_READ_CONSISTENCY: ReadConsistency = "weak"
