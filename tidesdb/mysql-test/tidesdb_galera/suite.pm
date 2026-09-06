package My::Suite::TidesDB_Galera;

use warnings;
use lib 'suite';
use wsrep::common;

@ISA = qw(My::Suite);

return wsrep_not_ok() if wsrep_not_ok();

# The benign wsrep startup lines a fresh two-node cluster always logs: a node
# advertising its loopback sst address, and the first-boot absence of the saved
# state files.  The server galera suite suppresses the same ones.
push @::global_suppressions,
  (
     qr(WSREP: wsrep_sst_receive_address is set to '127\.0\.0\.1),
     qr(WSREP: Could not open saved state file for reading: ),
     qr(WSREP: Could not open state file for reading: ),
     qr(WSREP: Gap in state sequence\. Need state transfer\.),
     qr(WSREP: Failed to prepare for incremental state transfer:),
     qr|WSREP: access file\(.*gvwstate.dat\) failed ?\(No such file or directory\)|,
     qr(WSREP: Quorum: No node with complete state),
     qr(WSREP: Failed to send state UUID:),
     qr(WSREP: .*down context.*),
     qr(WSREP: last inactive check more than .+ skipping check),
     qr|WSREP: discarding established \(time wait\) |,
  );

bless {};
