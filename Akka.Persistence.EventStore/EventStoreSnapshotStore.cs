using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Akka.Event;
using Akka.Persistence.Snapshot;
using Akka.Serialization;
using EventStore.ClientAPI;
using Newtonsoft.Json;

namespace Akka.Persistence.EventStore
{
    public class EventStoreSnapshotStore : SnapshotStore
    {
        private readonly ILoggingAdapter _log = Logging.GetLogger(Context);
        private readonly EventStorePersistenceExtension _extension;
        private Lazy<Task<IEventStoreConnection>> _connection;
        private Serializer _serializer;

        public EventStoreSnapshotStore()
        {
            _log = Context.GetLogger();
            _extension = EventStorePersistence.Instance.Apply(Context.System);
        }

        protected override void PreStart()
        {
            base.PreStart();

            var serialization = Context.System.Serialization;
            _serializer = serialization.FindSerializerForType(typeof(SelectedSnapshot));

            _connection = new Lazy<Task<IEventStoreConnection>>(async () =>
            {
                IEventStoreConnection connection = EventStoreConnection.Create(_extension.EventStoreSnapshotSettings.ConnectionString, _extension.EventStoreSnapshotSettings.ConnectionName);
                await connection.ConnectAsync();
                return connection;
            });
        }

        private Task<IEventStoreConnection> GetConnection()
        {
            return _connection.Value;
        }

        protected override async Task<SelectedSnapshot> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            var connection = await GetConnection();
            var streamName = GetStreamName(persistenceId);
            var requestedSnapVersion = (int)criteria.MaxSequenceNr;
            StreamEventsSlice slice = null;
            if (criteria.MaxSequenceNr == long.MaxValue)
            {
                requestedSnapVersion = StreamPosition.End;
                slice = await connection.ReadStreamEventsBackwardAsync(streamName, requestedSnapVersion, 1, false);
            }
            else
            {
                slice = await connection.ReadStreamEventsBackwardAsync(streamName, StreamPosition.End, requestedSnapVersion, false);
            }

            if (slice.Status == SliceReadStatus.StreamNotFound)
            {
                await connection.SetStreamMetadataAsync(streamName, ExpectedVersion.Any, StreamMetadata.Data);
                return null;
            }

            if (slice.Events.Any())
            {
                _log.Debug("Found snapshot of {0}", persistenceId);
                if (requestedSnapVersion == StreamPosition.End)
                {
                    var @event = slice.Events.First().OriginalEvent;
                    return (SelectedSnapshot)_serializer.FromBinary(@event.Data, typeof(SelectedSnapshot));
                }
                else
                {
                    var @event = slice.Events.Where(t => t.OriginalEvent.EventNumber == requestedSnapVersion).First().OriginalEvent;
                    return (SelectedSnapshot)_serializer.FromBinary(@event.Data, typeof(SelectedSnapshot));
                }
            }

            return null;
        }

        private static string GetStreamName(string persistenceId)
        {
            return string.Format("snapshot-{0}", persistenceId);
        }

        protected override async Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            var connection = await GetConnection();
            var streamName = GetStreamName(metadata.PersistenceId);
            var data = _serializer.ToBinary(new SelectedSnapshot(metadata, snapshot));
            var eventData = new EventData(Guid.NewGuid(), typeof(Serialization.Snapshot).Name, false, data, new byte[0]);

            await connection.AppendToStreamAsync(streamName, ExpectedVersion.Any, eventData);
        }

        protected override Task DeleteAsync(SnapshotMetadata metadata)
        {
            // no-op
            return Task.Run(() => { });
        }

        protected override Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            // no-op
            return Task.Run(() => { });
        }

        public class StreamMetadata
        {
            [JsonProperty("$maxCount")]
            public int MaxCount = 1;

            private StreamMetadata()
            {
            }

            private static readonly StreamMetadata Instance = new StreamMetadata();

            public static byte[] Data
            {
                get
                {
                    return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(Instance));
                }
            }
        }
    }
}