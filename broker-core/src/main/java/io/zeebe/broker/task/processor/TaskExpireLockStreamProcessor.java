/**
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.task.processor;

import static io.zeebe.protocol.clientapi.EventType.TASK_EVENT;
import static org.agrona.BitUtil.SIZE_OF_INT;

import java.util.HashMap;

import org.agrona.DirectBuffer;

import io.zeebe.broker.Constants;
import io.zeebe.broker.logstreams.BrokerEventMetadata;
import io.zeebe.broker.logstreams.processor.MetadataFilter;
import io.zeebe.broker.task.data.TaskEvent;
import io.zeebe.broker.task.data.TaskEventType;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamReader;
import io.zeebe.logstreams.log.LogStreamWriter;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.logstreams.processor.EventProcessor;
import io.zeebe.logstreams.processor.StreamProcessor;
import io.zeebe.logstreams.processor.StreamProcessorContext;
import io.zeebe.logstreams.snapshot.SerializableWrapper;
import io.zeebe.logstreams.spi.SnapshotSupport;
import io.zeebe.protocol.clientapi.EventType;
import io.zeebe.util.DeferredCommandContext;
import io.zeebe.util.time.ClockUtil;

public class TaskExpireLockStreamProcessor implements StreamProcessor
{
    protected static final int INDEX_VALUE_LENGTH = SIZE_OF_INT + SIZE_OF_INT;

    protected final EventProcessor lockedEventProcessor = new LockedEventProcessor();
    protected final EventProcessor unlockEventProcessor = new UnlockEventProcessor();
    protected final EventProcessor expireLockEventProcessor = new ExpireLockEventProcessor();

    protected final Runnable checkLockExpirationCmd = new CheckLockExpirationCmd();

    // TODO #161 - replace the index by a more efficient one
    protected HashMap<Long, ExpirationTimeBucket> index = new HashMap<>();
    protected SerializableWrapper<HashMap<Long, ExpirationTimeBucket>> indexSnapshot = new SerializableWrapper<>(index);

    protected DeferredCommandContext cmdQueue;

    protected LogStreamReader targetLogStreamReader;
    protected LogStreamWriter targetLogStreamWriter;

    protected LogStream targetStream;
    protected DirectBuffer targetLogStreamTopicName;
    protected int targetLogStreamPartitionId;
    protected int streamProcessorId;

    protected final BrokerEventMetadata targetEventMetadata = new BrokerEventMetadata();
    protected final TaskEvent taskEvent = new TaskEvent();
    protected long eventKey = 0;
    protected long eventPosition = 0;

    protected long lastWrittenEventPosition = 0;

    @Override
    public SnapshotSupport getStateResource()
    {
        return indexSnapshot;
    }

    @Override
    public void onOpen(StreamProcessorContext context)
    {
        streamProcessorId = context.getId();
        cmdQueue = context.getStreamProcessorCmdQueue();
        targetLogStreamReader = context.getTargetLogStreamReader();
        targetLogStreamWriter = context.getLogStreamWriter();

        targetStream = context.getTargetStream();
        targetLogStreamTopicName = targetStream.getTopicName();
        targetLogStreamPartitionId = targetStream.getPartitionId();

        // restore index from snapshot
        index = indexSnapshot.getObject();
    }

    public static MetadataFilter eventFilter()
    {
        return (m) -> m.getEventType() == EventType.TASK_EVENT;
    }

    @Override
    public EventProcessor onEvent(LoggedEvent event)
    {
        eventKey = event.getKey();
        eventPosition = event.getPosition();

        taskEvent.reset();
        event.readValue(taskEvent);

        EventProcessor eventProcessor = null;

        switch (taskEvent.getEventType())
        {
            case LOCKED:
                eventProcessor = lockedEventProcessor;
                break;
            case EXPIRE_LOCK:
                eventProcessor = expireLockEventProcessor;
                break;
            case LOCK_EXPIRED:
            case COMPLETED:
            case FAILED:
                eventProcessor = unlockEventProcessor;
                break;

            default:
                break;
        }
        return eventProcessor;
    }

    class LockedEventProcessor implements EventProcessor
    {

        @Override
        public void processEvent()
        {
            // just add event to index
        }

        @Override
        public void updateState()
        {
            final ExpirationTimeBucket expirationTimeBucket = new ExpirationTimeBucket(eventPosition, taskEvent.getLockTime());
            index.put(eventKey, expirationTimeBucket);
        }

    }

    class UnlockEventProcessor implements EventProcessor
    {

        @Override
        public void processEvent()
        {
            // just remove event from index
        }

        @Override
        public void updateState()
        {
            index.remove(eventKey);
        }

    }

    class ExpireLockEventProcessor implements EventProcessor
    {

        @Override
        public void processEvent()
        {
            // just process the previous written event for writing a snapshot afterwards
        }

        @Override
        public long writeEvent(LogStreamWriter writer)
        {
            // returns the position of the previous written event
            return lastWrittenEventPosition;
        }

    }

    public void checkLockExpirationAsync()
    {
        cmdQueue.runAsync(checkLockExpirationCmd);
    }

    class CheckLockExpirationCmd implements Runnable
    {

        @Override
        public void run()
        {
            if (index.size() > 0)
            {
                for (long eventKey : index.keySet())
                {
                    final ExpirationTimeBucket expirationTimeBucket = index.get(eventKey);

                    final long eventPosition = expirationTimeBucket.getEventPosition();
                    final long lockExpirationTime = expirationTimeBucket.getExpirationTime();

                    checkLockExpirationTime(eventKey, eventPosition, lockExpirationTime);
                }
            }
        }

        protected void checkLockExpirationTime(long eventKey, final long eventPosition, final long lockExpirationTime)
        {
            if (lockExpirationTime <= ClockUtil.getCurrentTimeInMillis())
            {
                final boolean found = targetLogStreamReader.seek(eventPosition);
                if (found && targetLogStreamReader.hasNext())
                {
                    final LoggedEvent lockedEvent = targetLogStreamReader.next();

                    writeLockExpireEvent(eventKey, lockedEvent);
                }
                else
                {
                    throw new IllegalStateException("Failed to check the task lock expiration time. Indexed task event not found in log stream.");
                }
            }
        }

        protected void writeLockExpireEvent(long eventKey, final LoggedEvent lockedEvent)
        {
            taskEvent.reset();
            lockedEvent.readValue(taskEvent);

            taskEvent.setEventType(TaskEventType.EXPIRE_LOCK);

            targetEventMetadata
                .reset()
                .protocolVersion(Constants.PROTOCOL_VERSION)
                .eventType(TASK_EVENT)
                .raftTermId(targetStream.getTerm());

            final long position = targetLogStreamWriter
                    .producerId(streamProcessorId)
                    .sourceEvent(targetLogStreamTopicName, targetLogStreamPartitionId, lockedEvent.getPosition())
                    .key(eventKey)
                    .metadataWriter(targetEventMetadata)
                    .valueWriter(taskEvent)
                    .tryWrite();

            if (position >= 0)
            {
                lastWrittenEventPosition = position;

                index.remove(eventKey);
            }
        }
    }

}