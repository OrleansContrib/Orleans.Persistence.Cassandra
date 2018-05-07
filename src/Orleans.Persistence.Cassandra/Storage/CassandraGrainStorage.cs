using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Newtonsoft.Json;

using Orleans.Configuration;
using Orleans.Persistence.Cassandra.Models;
using Orleans.Persistence.Cassandra.Options;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.Persistence.Cassandra.Storage
{
    internal sealed class CassandraGrainStorage : IGrainStorage, ILifecycleParticipant<ISiloLifecycle>
    {
        private const ConsistencyLevel SerialConsistencyLevel = ConsistencyLevel.Serial;

        private readonly string _name;
        private readonly string _serviceId;
        private readonly CassandraStorageOptions _cassandraStorageOptions;
        private readonly ILogger<CassandraGrainStorage> _logger;
        private readonly IGrainFactory _grainFactory;
        private readonly ITypeResolver _typeResolver;

        private JsonSerializerSettings _jsonSettings;
        private Table<CassandraGrainState> _dataTable;
        private Mapper _mapper;

        public CassandraGrainStorage(
            string name,
            CassandraStorageOptions cassandraStorageOptions,
            IOptions<ClusterOptions> clusterOptions,
            ILogger<CassandraGrainStorage> logger,
            IGrainFactory grainFactory,
            ITypeResolver typeResolver)
        {
            _name = name;
            _serviceId = clusterOptions.Value.ServiceId;
            _cassandraStorageOptions = cassandraStorageOptions;
            _logger = logger;
            _grainFactory = grainFactory;
            _typeResolver = typeResolver;
        }

        public async Task ReadStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            var (_, cassandraState) = await GetCassandraGrainState(grainType, grainReference);
            if (cassandraState != null)
            {
                grainState.State = JsonConvert.DeserializeObject<object>(cassandraState.State, _jsonSettings);
                grainState.ETag = cassandraState.ETag;
            }
        }

        public async Task WriteStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            var (id, cassandraState) = await GetCassandraGrainState(grainType, grainReference);
            var newEtag = 0;
            try
            {
                var json = JsonConvert.SerializeObject(grainState.State, _jsonSettings);

                if (cassandraState == null)
                {
                    cassandraState = new CassandraGrainState
                        {
                            Id = id,
                            GrainType = grainType,
                            State = json,
                            ETag = newEtag.ToString()
                        };

                    await _mapper.InsertIfNotExistsAsync(cassandraState);
                }
                else
                {
                    int.TryParse(grainState.ETag, out var stateEtag);
                    newEtag = stateEtag;
                    newEtag++;

                    var appliedInfo =
                        await _mapper.UpdateIfAsync<CassandraGrainState>(
                                         Cql.New(
                                                $"SET {nameof(CassandraGrainState.State)} = ?, {nameof(CassandraGrainState.ETag)} = ? " +
                                                $"WHERE {nameof(CassandraGrainState.Id)} = ? AND {nameof(CassandraGrainState.GrainType)} = ? " +
                                                $"IF {nameof(CassandraGrainState.ETag)} = ?",
                                                json,
                                                newEtag.ToString(),
                                                id,
                                                grainType,
                                                stateEtag.ToString())
                                            .WithOptions(x => x.SetSerialConsistencyLevel(SerialConsistencyLevel)))
                                     .ConfigureAwait(false);

                    if (!appliedInfo.Applied)
                    {
                        throw new CassandraConcurrencyException(cassandraState.Id, stateEtag.ToString(), appliedInfo.Existing.ETag);
                    }
                }

                grainState.ETag = newEtag.ToString();
            }
            catch (DriverException)
            {
                _logger.LogWarning("Cassandra driver error occured while creating grain state for grain {grainId}.", id);
                throw;
            }
        }

        public async Task ClearStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            var (id, _) = await GetCassandraGrainState(grainType, grainReference);
            try
            {
                if (_cassandraStorageOptions.DeleteStateOnClear)
                {
                    await _mapper.DeleteAsync<CassandraGrainState>(
                                     Cql.New(
                                            $"WHERE {nameof(CassandraGrainState.Id)} = ? AND {nameof(CassandraGrainState.GrainType)} = ?",
                                            id,
                                            grainType)
                                        .WithOptions(x => x.SetSerialConsistencyLevel(SerialConsistencyLevel)))
                                 .ConfigureAwait(false);

                    grainState.ETag = string.Empty;
                }
                else
                {
                    var json = JsonConvert.SerializeObject(grainState.State, _jsonSettings);

                    int.TryParse(grainState.ETag, out var stateEtag);
                    var newEtag = stateEtag;
                    newEtag++;

                    var appliedInfo =
                        await _mapper.UpdateIfAsync<CassandraGrainState>(
                                         Cql.New(
                                                $"SET {nameof(CassandraGrainState.State)} = ?, {nameof(CassandraGrainState.ETag)} = ? " +
                                                $"WHERE {nameof(CassandraGrainState.Id)} = ? AND {nameof(CassandraGrainState.GrainType)} = ? " +
                                                $"IF {nameof(CassandraGrainState.ETag)} = ?",
                                                json,
                                                newEtag.ToString(),
                                                id,
                                                grainType,
                                                stateEtag.ToString())
                                            .WithOptions(x => x.SetSerialConsistencyLevel(SerialConsistencyLevel)))
                                     .ConfigureAwait(false);

                    if (!appliedInfo.Applied)
                    {
                        throw new CassandraConcurrencyException(id, stateEtag.ToString(), appliedInfo.Existing.ETag);
                    }

                    grainState.ETag = newEtag.ToString();
                }
            }
            catch (DriverException)
            {
                _logger.LogWarning("Cassandra driver error occured while clearing grain state for grain {grainId}.", id);
                throw;
            }
        }

        public void Participate(ISiloLifecycle lifecycle)
            => lifecycle.Subscribe(OptionFormattingUtilities.Name<CassandraGrainStorage>(_name), _cassandraStorageOptions.InitStage, Init, Close);

        private string GetKeyString(GrainReference grainReference) => $"{_serviceId}_{grainReference.ToKeyString()}";

        private async Task<(string, CassandraGrainState)> GetCassandraGrainState(string grainType, GrainReference grainReference)
        {
            var id = GetKeyString(grainReference);
            try
            {
                var state = await _mapper.FirstOrDefaultAsync<CassandraGrainState>(
                                             Cql.New(
                                                    $"WHERE {nameof(CassandraGrainState.Id)} = ? AND {nameof(CassandraGrainState.GrainType)} = ?",
                                                    id,
                                                    grainType)
                                                .WithOptions(x => x.SetSerialConsistencyLevel(SerialConsistencyLevel)))
                                         .ConfigureAwait(false);

                return (id, state);
            }
            catch (DriverException)
            {
                _logger.LogWarning("Cassandra driver error occured while reading grain state for grain {grainId}.", id);
                throw;
            }
        }

        private async Task Init(CancellationToken cancellationToken)
        {
            try
            {

                _jsonSettings = OrleansJsonSerializer.GetDefaultSerializerSettings(_typeResolver, _grainFactory);
                _jsonSettings.TypeNameHandling = _cassandraStorageOptions.Serialization.TypeNameHandling;
                _jsonSettings.MetadataPropertyHandling = _cassandraStorageOptions.Serialization.MetadataPropertyHandling;
                if (_cassandraStorageOptions.Serialization.UseFullAssemblyNames)
                {
                    _jsonSettings.TypeNameAssemblyFormatHandling = TypeNameAssemblyFormatHandling.Full;
                }

                if (_cassandraStorageOptions.Serialization.IndentJson)
                {
                    _jsonSettings.Formatting = Formatting.Indented;
                }

                var cassandraOptions = _cassandraStorageOptions;
                var cassandraCluster =
                    Cluster.Builder()
                           .AddContactPoints(cassandraOptions.ContactPoints)
                           .WithDefaultKeyspace(cassandraOptions.Keyspace)
                           .Build();

                var session = cassandraCluster.ConnectAndCreateDefaultKeyspaceIfNotExists(
                    new Dictionary<string, string>
                        {
                            { "class", "SimpleStrategy" },
                            { "replication_factor", cassandraOptions.ReplicationFactor.ToString() }
                        });

                var mappingConfiguration = new MappingConfiguration().Define(new EntityMappings(cassandraOptions.TableName));

                _dataTable = new Table<CassandraGrainState>(session, mappingConfiguration);
                await Task.Run(() => _dataTable.CreateIfNotExists(), cancellationToken).ConfigureAwait(false);

                _mapper = new Mapper(session, mappingConfiguration);
            }
            catch (DriverException)
            {
                _logger.LogWarning("Cassandra driver error occured while initializing grain storage for service {serviceId}.", _serviceId);
                throw;
            }
        }

        private Task Close(CancellationToken cancellationToken)
        {
            _dataTable.GetSession().Dispose();
            return Task.CompletedTask;
        }
    }

    public static class CassandraGrainStorageFactory
    {
        public static IGrainStorage Create(IServiceProvider services, string name)
        {
            var optionsSnapshot = services.GetRequiredService<IOptionsSnapshot<CassandraStorageOptions>>();
            return ActivatorUtilities.CreateInstance<CassandraGrainStorage>(services, optionsSnapshot.Get(name), name);
        }
    }
}