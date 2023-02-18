using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

using Orleans.Configuration;
using Orleans.GrainReferences;
using Orleans.Persistence.Cassandra.Concurrency;
using Orleans.Persistence.Cassandra.Models;
using Orleans.Persistence.Cassandra.Options;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Serialization.Buffers;
using Orleans.Storage;

namespace Orleans.Persistence.Cassandra.Storage
{
    internal sealed class CassandraGrainStorage : IGrainStorage, ILifecycleParticipant<ISiloLifecycle>
    {
        private const ConsistencyLevel SerialConsistencyLevel = ConsistencyLevel.Serial;
        private const ConsistencyLevel DefaultConsistencyLevel = ConsistencyLevel.Quorum;

        private static readonly CqlQueryOptions SerialConsistencyQueryOptions =
            CqlQueryOptions.New().SetSerialConsistencyLevel(SerialConsistencyLevel);

        private static readonly CqlQueryOptions DefaultConsistencyQueryOptions =
            CqlQueryOptions.New().SetConsistencyLevel(DefaultConsistencyLevel);

        private readonly string _name;
        private readonly string _serviceId;
        private readonly CassandraStorageOptions _cassandraStorageOptions;
        private readonly ILogger<CassandraGrainStorage> _logger;
        //private readonly IGrainFactory _grainFactory;
        private readonly IServiceProvider _services;
        //private readonly ITypeResolver _typeResolver;
        private readonly HashSet<Type> _concurrentStateTypes;

        private JsonSerializerSettings _jsonSettings;
        private Cluster _cluster;
        private Mapper _mapper;
                
        public CassandraGrainStorage(
            string name,
            CassandraStorageOptions cassandraStorageOptions,
            IOptions<ClusterOptions> clusterOptions,
            ILogger<CassandraGrainStorage> logger,
            IGrainFactory grainFactory,
            //ITypeResolver typeResolver,
            IConcurrentGrainStateTypesProvider concurrentGrainStateTypesProvider,
            IServiceProvider services,
            ILoggerProvider loggerProvider)
        {
            _name = name;
            _serviceId = clusterOptions.Value.ServiceId;
            _cassandraStorageOptions = cassandraStorageOptions;
            _logger = logger;
            _/*grainFactory*/ = grainFactory;
            //_typeResolver = typeResolver;
            _services = services;
            _concurrentStateTypes = new HashSet<Type>(concurrentGrainStateTypesProvider.GetGrainStateTypes());

            Diagnostics.CassandraPerformanceCountersEnabled = _cassandraStorageOptions.Diagnostics.PerformanceCountersEnabled;
            Diagnostics.CassandraStackTraceIncluded = _cassandraStorageOptions.Diagnostics.StackTraceIncluded;

            if (loggerProvider != null)
            {
                Diagnostics.AddLoggerProvider(loggerProvider);
            }
        }

        public async Task ReadStateAsync<T>(string grainType, GrainId grainReference, IGrainState<T> grainState)
        {
            var isConcurrentState = _concurrentStateTypes.Contains(grainState.State.GetType());
            var (_, cassandraState) = await GetCassandraGrainState(grainType, grainReference, isConcurrentState);
            if (cassandraState != null)
            {
                grainState.State = JsonConvert.DeserializeObject<T>(cassandraState.State, _jsonSettings);
                grainState.ETag = isConcurrentState ? cassandraState.ETag : string.Empty;
            }
        }

        public async Task WriteStateAsync<T>(string grainType, GrainId grainId, IGrainState<T> grainState)
        {
            var isConcurrentState = _concurrentStateTypes.Contains(grainState.State.GetType());
            var (id, cassandraState) = await GetCassandraGrainState(grainType, grainId, isConcurrentState);
            try
            {
                var json = JsonConvert.SerializeObject(grainState.State, _jsonSettings);
                if (isConcurrentState)
                {
                    var newEtag = 0;
                    if (cassandraState == null)
                    {
                        cassandraState = new CassandraGrainState
                            {
                                Id = id,
                                GrainType = grainType,
                                State = json,
                                ETag = newEtag.ToString()
                            };

                        await _mapper.InsertIfNotExistsAsync(cassandraState, SerialConsistencyQueryOptions)
                                     .ConfigureAwait(false);
                    }
                    else
                    {
                        if( !int.TryParse(grainState.ETag, out var stateEtag))
                            stateEtag = 0;
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
                else
                {
                    if (cassandraState == null)
                    {
                        cassandraState = new CassandraGrainState
                            {
                                Id = id,
                                GrainType = grainType,
                                State = json,
                                ETag = string.Empty
                            };

                        await _mapper.InsertAsync(cassandraState, DefaultConsistencyQueryOptions)
                                     .ConfigureAwait(false);
                    }
                    else
                    {
                        cassandraState.State = json;
                        await _mapper.UpdateAsync(cassandraState, DefaultConsistencyQueryOptions)
                                     .ConfigureAwait(false);
                    }
                }
            }
            catch (DriverException)
            {
                _logger.LogWarning("Cassandra driver error occured while creating grain state for grain {grainId}.", id);
                throw;
            }
        }

        public async Task ClearStateAsync<T>(string grainType, GrainId grainReference, IGrainState<T> grainState)
        {
            var isConcurrentState = _concurrentStateTypes.Contains(grainState.State.GetType());
            var (id, cassandraState) = await GetCassandraGrainState(grainType, grainReference, isConcurrentState);
            try
            {
                if (_cassandraStorageOptions.DeleteStateOnClear)
                {
                    await _mapper.DeleteAsync<CassandraGrainState>(
                                     Cql.New(
                                            $"WHERE {nameof(CassandraGrainState.Id)} = ? AND {nameof(CassandraGrainState.GrainType)} = ?",
                                            id,
                                            grainType)
                                        .WithOptions(
                                            x =>
                                                {
                                                    if (isConcurrentState)
                                                    {
                                                        x.SetSerialConsistencyLevel(SerialConsistencyLevel);
                                                    }
                                                    else
                                                    {
                                                        x.SetConsistencyLevel(DefaultConsistencyLevel);
                                                    }
                                                }))
                                 .ConfigureAwait(false);

                    grainState.ETag = string.Empty;
                }
                else
                {
                    var json = JsonConvert.SerializeObject(grainState.State, _jsonSettings);
                    if (isConcurrentState)
                    {
                        if(!int.TryParse(grainState.ETag, out var stateEtag))
                            stateEtag = 0;
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
                    else
                    {
                        cassandraState.State = json;
                        await _mapper.UpdateAsync(cassandraState, DefaultConsistencyQueryOptions)
                                     .ConfigureAwait(false);
                    }
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

        private string GetKeyString(GrainId grainId) => $"{_serviceId}_{grainId}";

        private async Task<(string, CassandraGrainState)> GetCassandraGrainState(
            string grainType,
            GrainId grainReference,
            bool isConcurrentState)
        {
            var id = GetKeyString(grainReference);
            try
            {
                var state = await _mapper.FirstOrDefaultAsync<CassandraGrainState>(
                                             Cql.New(
                                                    $"WHERE {nameof(CassandraGrainState.Id)} = ? AND {nameof(CassandraGrainState.GrainType)} = ?",
                                                    id,
                                                    grainType)
                                                .WithOptions(
                                                    x =>
                                                        {
                                                            if (isConcurrentState)
                                                            {
                                                                x.SetSerialConsistencyLevel(SerialConsistencyLevel);
                                                            }
                                                            else
                                                            {
                                                                x.SetConsistencyLevel(DefaultConsistencyLevel);
                                                            }
                                                        }))
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
                _jsonSettings = OrleansJsonSerializerSettings.GetDefaultSerializerSettings(_services/*_typeResolver, _grainFactory*/);
                _jsonSettings.TypeNameHandling = _cassandraStorageOptions.JsonSerialization.TypeNameHandling;
                _jsonSettings.MetadataPropertyHandling = _cassandraStorageOptions.JsonSerialization.MetadataPropertyHandling;

                // clear and fill again to work around "Unable to cast object ... to type 'Orleans.Runtime.GrainReference'."
                //    bei Orleans.Serialization.GrainReferenceJsonConverter.WriteJson(JsonWriter writer, Object value, JsonSerializer serializer)
                //    bei Newtonsoft.Json.Serialization.JsonSerializerInternalWriter.SerializeConvertable(JsonWriter writer, JsonConverter converter, Object value, JsonContract contract, JsonContainerContract collectionContract, JsonProperty containerProperty)
                //    bei Newtonsoft.Json.Serialization.JsonSerializerInternalWriter.SerializeValue(JsonWriter writer, Object value, JsonContract valueContract, JsonProperty member, JsonContainerContract containerContract, JsonProperty containerProperty)
                //    bei Newtonsoft.Json.Serialization.JsonSerializerInternalWriter.SerializeObject(JsonWriter writer, Object value, JsonObjectContract contract, JsonProperty member, JsonContainerContract collectionContract, JsonProperty containerProperty)
                //    bei Newtonsoft.Json.Serialization.JsonSerializerInternalWriter.SerializeValue(JsonWriter writer, Object value, JsonContract valueContract, JsonProperty member, JsonContainerContract containerContract, JsonProperty containerProperty)
                //    bei Newtonsoft.Json.Serialization.JsonSerializerInternalWriter.Serialize(JsonWriter jsonWriter, Object value, Type objectType)
                //    bei Newtonsoft.Json.JsonSerializer.SerializeInternal(JsonWriter jsonWriter, Object value, Type objectType)
                //    bei Newtonsoft.Json.JsonSerializer.Serialize(JsonWriter jsonWriter, Object value, Type objectType)
                //    bei Newtonsoft.Json.JsonConvert.SerializeObjectInternal(Object value, Type type, JsonSerializer jsonSerializer)
                //    bei Newtonsoft.Json.JsonConvert.SerializeObject(Object value, Type type, JsonSerializerSettings settings)
                //    bei Newtonsoft.Json.JsonConvert.SerializeObject(Object value, JsonSerializerSettings settings)
                //    bei Orleans.Persistence.Cassandra.Storage.CassandraGrainStorage.< WriteStateAsync > d__15`1.MoveNext() in .\lib\3rdparty\Orleans.Persistence.Cassandra\src\Orleans.Persistence.Cassandra\Storage\CassandraGrainStorage.cs: Zeile99
                _jsonSettings.Converters.Clear();
                _jsonSettings.Converters.Add(new IPAddressConverter());
                _jsonSettings.Converters.Add(new IPEndPointConverter());
                _jsonSettings.Converters.Add(new GrainIdConverter());
                _jsonSettings.Converters.Add(new Serialization.ActivationIdConverter());
                _jsonSettings.Converters.Add(new SiloAddressJsonConverter());
                _jsonSettings.Converters.Add(new MembershipVersionJsonConverter());
                _jsonSettings.Converters.Add(new UniqueKeyConverter());
                //_jsonSettings.Converters.Add(new GrainReferenceJsonConverter(services.GetRequiredService<GrainReferenceActivator>()));
                _jsonSettings.Converters.Add(new MyGrainReferenceJsonConverter(_services.GetRequiredService<GrainReferenceActivator>()));


                if (_cassandraStorageOptions.JsonSerialization.ContractResolver != null)
                {
                    _jsonSettings.ContractResolver = _cassandraStorageOptions.JsonSerialization.ContractResolver;
                }

                if (_cassandraStorageOptions.JsonSerialization.UseFullAssemblyNames)
                {
                    _jsonSettings.TypeNameAssemblyFormatHandling = TypeNameAssemblyFormatHandling.Full;
                }

                if (_cassandraStorageOptions.JsonSerialization.IndentJson)
                {
                    _jsonSettings.Formatting = Formatting.Indented;
                }

                /*
                var certCollection = new System.Security.Cryptography.X509Certificates.X509Certificate2Collection();
                var amazoncert = new System.Security.Cryptography.X509Certificates.X509Certificate2("./sf-class2-root.crt");
                certCollection.Add(amazoncert);
                */

                 var cassandraOptions = _cassandraStorageOptions;
                _cluster = Cluster.Builder()
                                  .AddContactPoints(cassandraOptions.ContactPoints.Split(','))
                                  //.WithPort(9142)
                                  //.WithAuthProvider(new PlainTextAuthProvider("user", "password"))
                                  //.WithSSL(new SSLOptions().SetCertificateCollection(certCollection))
                                  .Build();

                var session = await _cluster.ConnectAsync();
                await Task.Run(
                              () =>
                                  {
                                      var keyspace = cassandraOptions.Keyspace;
                                      session.CreateKeyspaceIfNotExists(
                                          keyspace,
                                          new Dictionary<string, string>
                                              {
                                                  { "class", "SimpleStrategy" },
                                                  { "replication_factor", cassandraOptions.ReplicationFactor.ToString() }
                                              });
                                  },
                              cancellationToken)
                          .ConfigureAwait(false);

                var mappingConfiguration = new MappingConfiguration().Define(new EntityMappings(cassandraOptions.Keyspace, cassandraOptions.TableName));

                await Task.Run(
                              async () =>
                                  {
                                      var grainStateTable = new Table<CassandraGrainState>(session, mappingConfiguration);

                                      var systemTableTable = new Table<CassandraSystemTables>(session, mappingConfiguration);
                                      var result = await systemTableTable.FirstOrDefault(
                                                                             t => t.KeyspaceName == grainStateTable.KeyspaceName &&
                                                                                  t.TableName == grainStateTable.Name)
                                                                         .ExecuteAsync();
                                      if (result == null)
                                      {
                                          grainStateTable.Create();
                                      }
                                  },
                              cancellationToken)
                          .ConfigureAwait(false);

                _mapper = new Mapper(session, mappingConfiguration);
            }
            catch (DriverException)
            {
                _logger.LogWarning("Cassandra driver error occured while initializing grain storage for service {serviceId}.", _serviceId);
                throw;
            }
        }

        private async Task Close(CancellationToken _) => await _cluster.ShutdownAsync();
    }

    public static class CassandraGrainStorageFactory
    {
        public static IGrainStorage Create(IServiceProvider services, string name)
        {
            var optionsSnapshot = services.GetRequiredService<IOptionsSnapshot<CassandraStorageOptions>>();
            var typesProvider = services.GetRequiredServiceByName<IConcurrentGrainStateTypesProvider>(name);
            return ActivatorUtilities.CreateInstance<CassandraGrainStorage>(services, name, optionsSnapshot.Get(name), typesProvider, services);
        }
    }
}