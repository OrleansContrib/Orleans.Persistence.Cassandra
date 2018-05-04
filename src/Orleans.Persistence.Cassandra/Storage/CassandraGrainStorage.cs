using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Orleans.Configuration;
using Orleans.Persistence.Cassandra.Models;
using Orleans.Persistence.Cassandra.Options;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Persistence.Cassandra.Storage
{
    internal sealed class CassandraGrainStorage : IGrainStorage, ILifecycleParticipant<ISiloLifecycle>
    {
        private readonly string _name;
        private readonly string _serviceId;
        private readonly CassandraStorageOptions _cassandraStorageOptions;
        private readonly ILogger<CassandraGrainStorage> _logger;

        private Table<GrainState> _dataTable;
        private Mapper _mapper;

        public CassandraGrainStorage(
            string name,
            CassandraStorageOptions cassandraStorageOptions,
            IOptions<ClusterOptions> clusterOptions,
            ILogger<CassandraGrainStorage> logger)
        {
            _name = name;
            _serviceId = clusterOptions.Value.ServiceId;
            _cassandraStorageOptions = cassandraStorageOptions;
            _logger = logger;
        }

        public async Task ReadStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            var (_, state) = await GetGrainState(grainType, grainReference);
            if (state != null)
            {
                grainState.State = state.State;
                grainState.ETag = state.ETag;
            }
        }

        public async Task WriteStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            var (id, state) = await GetGrainState(grainType, grainReference);
            var newEtag = 0;
            try
            {
                if (state == null)
                {
                    state = new GrainState
                        {
                            Id = id,
                            GrainType = grainType,
                            State = grainState.State.ToString(),
                            ETag = newEtag.ToString()
                        };

                    await _mapper.InsertIfNotExistsAsync(state);
                }
                else
                {
                    int.TryParse(grainState.ETag, out var currentEtag);
                    newEtag = currentEtag;
                    newEtag++;

                    state.State = grainState.ToString();
                    state.ETag = newEtag.ToString();

                    await _mapper.UpdateIfAsync<GrainState>(
                        Cql.New(
                               $"WHERE {nameof(GrainState.Id)} = ? AND {nameof(GrainState.GrainType)} = ? IF {nameof(GrainState.ETag)} = ?",
                               state.Id,
                               grainType,
                               currentEtag)
                           .WithOptions(x => x.SetSerialConsistencyLevel(ConsistencyLevel.Serial)));
                }
            }
            catch (Exception)
            {
                _logger.LogWarning("Cassandra driver error occured while creating grain state for grain {grainId}.", id);
                throw;
            }
        }

        public Task ClearStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            throw new NotImplementedException();
        }

        public void Participate(ISiloLifecycle lifecycle)
        {
            lifecycle.Subscribe(OptionFormattingUtilities.Name<CassandraGrainStorage>(_name), _cassandraStorageOptions.InitStage, Init, Close);
        }

        private string GetKeyString(GrainReference grainReference) => $"{_serviceId}_{grainReference.ToKeyString()}";

        private async Task<(string, GrainState)> GetGrainState(string grainType, GrainReference grainReference)
        {
            var id = GetKeyString(grainReference);
            try
            {
                var state = await _mapper.SingleOrDefaultAsync<GrainState>(
                                             Cql.New(
                                                    $"WHERE {nameof(GrainState.Id)} = ? AND {nameof(GrainState.GrainType)} = ?",
                                                    id,
                                                    grainType)
                                                .WithOptions(x => x.SetSerialConsistencyLevel(ConsistencyLevel.Serial)))
                                         .ConfigureAwait(false);

                return (id, state);
            }
            catch (Exception)
            {
                _logger.LogWarning("Cassandra driver error occured while reading grain state for grain {grainId}.", id);
                throw;
            }
        }

        private async Task Init(CancellationToken cancellationToken)
        {
            try
            {
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

                _dataTable = new Table<GrainState>(session, mappingConfiguration);
                await Task.Run(() => _dataTable.CreateIfNotExists(), cancellationToken);

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