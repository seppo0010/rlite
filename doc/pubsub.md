# pubsub

## Functionallity

When a client subscribes to a channel, it should be able to receive messages
that will be published on that channel until it unsubscribes as fast as
possible. The API should allow clients to poll messages and block waiting for
new ones up to a timeout.

This somehow diverges from Redis implementation, since Redis responsability
is limited to writing in the connection socket, and polling or blocking is
up to the client, but in this case we are (also) the client.

To keep as much drop-in compatibility with existing Redis clients, it should
poll by default and an additional call should be implemented to block waiting
for messages.

Clients should be able to subscribe and unsubscribe to one or more channels and
patterns.

## Implementation

The communication must be asynchronous between the publisher and the
subscriptors, since the latter may not be reading at the moment.

There are three special databases added to rlite in order to support these
requirements. These databases are only available for pubsub.

Each connection has a random identifier (a `sha1` of the time when the
connection was open and a `rand()` call).

In the `subscribers special database` each key represents a channel, and it
contains a set where each value is one of the subscribers identifiers.

In the `psubscribers special database` each key represents a pattern, and it
contains a set where each value is one of the psubscribers identifiers.

When a publisher writes to a channel, it will fetch all the subscribers from
that channel, and from the matched patterns and rpush in the `messages
special database` to the subscriber connection identifier key with the number
of elements in the reply array and the same elements as Redis sends.
The number of elements is either 3 or 4 if the recipient has used subscribe or
psubscribe, since the latter includes both the pattern used and the channel
name.

Notice that opening an rlite connection for subscription requires write
permissions.

The subscribers will keep in memory a reference to the subscribed channels and
patterns to unsubscribe and delete left over data upon disconnect.

To block the read until new data is available posix fifo are used. The
subscriber creates a fifo file that is the database name with a "." prefix and
the connection identifier as a suffix, and calls `read` on it. The publisher
after writing the published messages will write to the fifo in order to signal
the recipient that new messages are available.

## Garbage collection

Healthy processes unsubscribe and clean up all its temporary subscriptions
when closing the database. However, if the process crashes or exits without
properly closing the database, temporary information will be persisted with no
further references.

<ideas wanted; the subscriptions may expire after some time and the process
are forced to re-subscribe? that might be alright if they are waiting for a
fifo `read()`, but the process might be doing anything else; is it ok to
lose some messages?>

# V2

All the previous concept apply. `connection identifier` is now called
`subscriptor identifier` and it is only created the first time the connection
is used to subscribe.

Subscriptions (both to channels and patterns) are persisted in disk, not just
in memory, in its own special database (a forth one).

When a subscriptor is created, a lock file is created and locked exclusively.
When a publisher will write to a subscriptor, it will check for its health
first by querying the status of the lock file. If it is not locked, the
publisher will instead do housekeeping work, eliminating all references to
that subscriptor (from channels, patterns, lock file and fifo if exists).

This will make sure reference may be cleaned up at some point.
