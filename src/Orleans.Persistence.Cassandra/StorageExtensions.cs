using System;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Persistence.Cassandra.Options;
using Orleans.Persistence.Cassandra.Storage;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Persistence.Cassandra
{
    public static class StorageExtensions
    {
        /// <summary>
        /// Configure silo to use Cassandra storage as the default grain storage.
        /// </summary>
        public static ISiloHostBuilder AddCassandraGrainStorageAsDefault(this ISiloHostBuilder builder, Action<CassandraStorageOptions> configureOptions)
        {
            return builder.AddCassandraGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>
        /// Configure silo to use Cassandra storage for grain storage.
        /// </summary>
        public static ISiloHostBuilder AddCassandraGrainStorage(this ISiloHostBuilder builder, string name, Action<CassandraStorageOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.AddCassandraGrainStorage(name, configureOptions));
        }

        /// <summary>
        /// Configure silo to use Cassandra storage as the default grain storage.
        /// </summary>
        public static ISiloHostBuilder AddCassandraGrainStorageAsDefault(this ISiloHostBuilder builder, Action<OptionsBuilder<CassandraStorageOptions>> configureOptions = null)
        {
            return builder.AddCassandraGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>
        /// Configure silo to use Cassandra storage for grain storage.
        /// </summary>
        public static ISiloHostBuilder AddCassandraGrainStorage(this ISiloHostBuilder builder, string name, Action<OptionsBuilder<CassandraStorageOptions>> configureOptions = null)
        {
            return builder.ConfigureServices(services => services.AddCassandraGrainStorage(name, configureOptions));
        }

        /// <summary>
        /// Configure silo to use Cassandra storage as the default grain storage.
        /// </summary>
        public static IServiceCollection AddCassandraGrainStorageAsDefault(this IServiceCollection services, Action<CassandraStorageOptions> configureOptions)
        {
            return services.AddCassandraGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, ob => ob.Configure(configureOptions));
        }

        /// <summary>
        /// Configure silo to use Cassandra storage for grain storage.
        /// </summary>
        public static IServiceCollection AddCassandraGrainStorage(this IServiceCollection services, string name, Action<CassandraStorageOptions> configureOptions)
        {
            return services.AddCassandraGrainStorage(name, ob => ob.Configure(configureOptions));
        }

        /// <summary>
        /// Configure silo to use Cassandra storage as the default grain storage.
        /// </summary>
        public static IServiceCollection AddCassandraGrainStorageAsDefault(this IServiceCollection services, Action<OptionsBuilder<CassandraStorageOptions>> configureOptions = null)
        {
            return services.AddCassandraGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }
        /// <summary>
        /// Configure silo to use Cassandra storage for grain storage.
        /// </summary>
        public static IServiceCollection AddCassandraGrainStorage(
            this IServiceCollection services,
            string name,
            Action<OptionsBuilder<CassandraStorageOptions>> configureOptions = null)
        {
            configureOptions?.Invoke(services.AddOptions<CassandraStorageOptions>(name));
            //services.AddTransient<IConfigurationValidator>(sp => new CosmosDBStorageOptionsValidator(sp.GetService<IOptionsSnapshot<CosmosDBStorageOptions>>().Get(name), name));
            services.ConfigureNamedOptionForLogging<CassandraStorageOptions>(name);
            services.TryAddSingleton(sp => sp.GetServiceByName<IGrainStorage>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME));

            return services.AddSingletonNamedService(name, CassandraGrainStorageFactory.Create)
                           .AddSingletonNamedService(name, (s, n) => (ILifecycleParticipant<ISiloLifecycle>)s.GetRequiredServiceByName<IGrainStorage>(n));
        }
    }
}