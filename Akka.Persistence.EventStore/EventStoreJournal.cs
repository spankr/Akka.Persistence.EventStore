using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.Serialization.Formatters;
using System.Text;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Persistence.Journal;
using EventStore.ClientAPI;
using EventStore.Persistence;
using Newtonsoft.Json;

namespace Akka.Persistence.EventStore
{
    public class EventStoreJournal : AsyncWriteJournal
    {
        private int _batchSize = 500;
        private readonly ILoggingAdapter _log = Logging.GetLogger(Context);
        private readonly EventStorePersistenceExtension _extension;
        private Lazy<Task<IEventStoreConnection>> _connection;
        private JsonSerializerSettings _serializerSettings;

        public EventStoreJournal()
        {
            _extension = EventStorePersistence.Instance.Apply(Context.System);
        }

        protected override void PreStart()
        {
            base.PreStart();

            _serializerSettings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.Objects,
                TypeNameAssemblyFormat = FormatterAssemblyStyle.Simple,
                Formatting = Formatting.Indented,
                Converters =
                {
                    new ActorRefConverter(Context)
                }
            };

            _connection = new Lazy<Task<IEventStoreConnection>>(async () =>
            {
                try
                {
                    IEventStoreConnection connection = EventStoreConnection.Create(_extension.EventStoreJournalSettings.ConnectionString, _extension.EventStoreJournalSettings.ConnectionName);
                    await connection.ConnectAsync();
                    return connection;
                }
                catch (Exception exc)
                {
                    _log.Error(exc.ToString());
                    return null;
                }
            });
        }

        private Task<IEventStoreConnection> GetConnection()
        {
            return _connection.Value;
        }

        public override async Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            try
            {
                var connection = await GetConnection();

                var slice = await connection.ReadStreamEventsBackwardAsync(persistenceId, StreamPosition.End, 1, false);

                long sequence = 0;

                if (slice.Events.Any())
                    sequence = slice.Events.First().OriginalEventNumber + 1;

                return sequence;
            }
            catch (Exception e)
            {
                _log.Error(e, e.Message);
                throw;
            }
        }

        public override async Task ReplayMessagesAsync(IActorContext context, string persistenceId, long fromSequenceNr, long toSequenceNr, long max, Action<IPersistentRepresentation> replayCallback)
        {
            try
            {
                if (toSequenceNr < fromSequenceNr || max == 0) return;
                if (fromSequenceNr == toSequenceNr) max = 1;
                if (toSequenceNr > fromSequenceNr && max == toSequenceNr) max = toSequenceNr - fromSequenceNr + 1;
                var connection = await GetConnection();
                long count = 0;
                var start = fromSequenceNr - 1;
                if (start < 0) start = 0;
                var localBatchSize = _batchSize;
                StreamEventsSlice slice;
                do
                {
                    if (max == long.MaxValue && toSequenceNr > fromSequenceNr)
                    {
                        max = toSequenceNr - fromSequenceNr + 1;
                    }
                    if (max < localBatchSize)
                    {
                        localBatchSize = (int)max;
                    }
                    slice = await connection.ReadStreamEventsForwardAsync(persistenceId, (int)start, localBatchSize, false);

                    foreach (var @event in slice.Events)
                    {
                        var json = Encoding.UTF8.GetString(@event.OriginalEvent.Data);
                        var representation = JsonConvert.DeserializeObject<IPersistentRepresentation>(json, _serializerSettings);
                        replayCallback(representation);
                        count++;
                        if (count == max) return;
                    }

                    start = slice.NextEventNumber;

                } while (!slice.IsEndOfStream);
            }
            catch (Exception e)
            {
                _log.Error(e, "Error replaying messages for: {0}", persistenceId);
                throw;
            }
        }

        protected override async Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<Akka.Persistence.AtomicWrite> messages)
        {
            var exceptions = new List<Exception>();
            foreach (var message in messages)
            {
                try
                {                
                    var persistentMessages = ((IImmutableList<IPersistentRepresentation>)message.Payload).ToArray();
                    foreach (var grouping in persistentMessages.GroupBy(x => x.PersistenceId))
                    {
                        var stream = grouping.Key;

                        var representations = grouping.OrderBy(x => x.SequenceNr).ToArray();
                        var expectedVersion = representations.First().SequenceNr - 2;

                        var events = representations.Select(x =>
                        {
                            var eventId = Guid.NewGuid();
                            var json = JsonConvert.SerializeObject(x, _serializerSettings);
                            var data = Encoding.UTF8.GetBytes(json);
                            var meta = new byte[0];
                            var payload = x.Payload;
                            if (payload.GetType().GetProperty("Metadata") != null)
                            {
                                var propType = payload.GetType().GetProperty("Metadata").PropertyType;
                                var metaJson =
                                    JsonConvert.SerializeObject(
                                        payload.GetType().GetProperty("Metadata").GetValue(x.Payload), propType,
                                        _serializerSettings);
                                meta = Encoding.UTF8.GetBytes(metaJson);
                            }
                            return new EventData(eventId, x.GetType().FullName, true, data, meta);
                        }).ToList(); // serilization issues don't bubble out from the AppentToStream call below, so enumerate the list before sending it to the call

                        var connection = await GetConnection();
                        await connection.AppendToStreamAsync(stream, (int)expectedVersion, events);
                    }
                    exceptions.Add(null);
                }
                catch (Exception e)
                {
                    exceptions.Add(e);
                }
            }


            return exceptions.ToImmutableList();
        }

        /// <summary>
        /// Delete is not supported in Event Store
        /// </summary>
        /// <param name="persistenceId"></param>
        /// <param name="toSequenceNr"></param>
        /// <returns></returns>
        protected override async Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            var connection = await GetConnection();
            var streamMetadataResult = await connection.GetStreamMetadataAsync(persistenceId);
            var newMetadata = streamMetadataResult.StreamMetadata.Copy().SetTruncateBefore((int)toSequenceNr).Build();
            await connection.SetStreamMetadataAsync(persistenceId, streamMetadataResult.MetastreamVersion, newMetadata);
        }

        class ActorRefConverter : JsonConverter
        {
            private readonly IActorContext _context;

            public ActorRefConverter(IActorContext context)
            {
                _context = context;
            }

            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                writer.WriteValue(((IActorRef)value).Path.ToStringWithAddress());
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                if (reader.TokenType == JsonToken.Null) return null;
                var value = reader.Value.ToString();

                ActorSelection selection = _context.ActorSelection(value);
                return selection.Anchor;
            }

            public override bool CanConvert(Type objectType)
            {
                return typeof(IActorRef).IsAssignableFrom(objectType);
            }
        }
    }
}