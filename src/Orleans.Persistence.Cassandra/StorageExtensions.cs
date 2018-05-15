using System;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Persistence.Cassandra.Concurrency;
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
        public static ISiloHostBuilder AddCassandraGrainStorageAsDefault(
            this ISiloHostBuilder builder,
            Func<IConfiguration, IConfiguration> configurationProvider,
            IConcurrentGrainStateTypesProvider concurrentGrainStateTypesProvider = null)
        {
            return builder.ConfigureServices((context, services) =>
                                                 {
                                                     services.AddCassandraGrainStorage(
                                                         ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME,
                                                         ob => ob.Bind(configurationProvider(context.Configuration)),
                                                         concurrentGrainStateTypesProvider);
                                                 });
        }

        /// <summary>
        /// Configure silo to use Cassandra storage for grain storage.
        /// </summary>
        public static ISiloHostBuilder AddCassandraGrainStorage(
            this ISiloHostBuilder builder,
            string name,
            Func<IConfiguration, IConfiguration> configurationProvider,
            IConcurrentGrainStateTypesProvider concurrentGrainStateTypesProvider = null)
        {
            return builder.ConfigureServices((context, services) =>
                                                 {
                                                     services.AddCassandraGrainStorage(
                                                         name,
                                                         ob => ob.Bind(configurationProvider(context.Configuration)),
                                                         concurrentGrainStateTypesProvider);
                                                 });
        }

        /// <summary>
        /// Configure silo to use Cassandra storage as the default grain storage.
        /// </summary>
        public static ISiloHostBuilder AddCassandraGrainStorageAsDefault(
            this ISiloHostBuilder builder,
            Action<CassandraStorageOptions> configureOptions,
            IConcurrentGrainStateTypesProvider concurrentGrainStateTypesProvider = null)
        {
            return builder.AddCassandraGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions, concurrentGrainStateTypesProvider);
        }

        /// <summary>
        /// Configure silo to use Cassandra storage for grain storage.
        /// </summary>
        public static ISiloHostBuilder AddCassandraGrainStorage(
            this ISiloHostBuilder builder,
            string name,
            Action<CassandraStorageOptions> configureOptions,
            IConcurrentGrainStateTypesProvider concurrentGrainStateTypesProvider = null)
        {
            return builder.ConfigureServices(services => services.AddCassandraGrainStorage(name, configureOptions, concurrentGrainStateTypesProvider));
        }

        /// <summary>
        /// Configure silo to use Cassandra storage as the default grain storage.
        /// </summary>
        public static ISiloHostBuilder AddCassandraGrainStorageAsDefault(
            this ISiloHostBuilder builder,
            Action<OptionsBuilder<CassandraStorageOptions>> configureOptions = null,
            IConcurrentGrainStateTypesProvider concurrentGrainStateTypesProvider = null)
        {
            return builder.AddCassandraGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions, concurrentGrainStateTypesProvider);
        }

        /// <summary>
        /// Configure silo to use Cassandra storage for grain storage.
        /// </summary>
        public static ISiloHostBuilder AddCassandraGrainStorage(
            this ISiloHostBuilder builder,
            string name,
            Action<OptionsBuilder<CassandraStorageOptions>> configureOptions = null,
            IConcurrentGrainStateTypesProvider concurrentGrainStateTypesProvider = null)
        {
            return builder.ConfigureServices(services => services.AddCassandraGrainStorage(name, configureOptions, concurrentGrainStateTypesProvider));
        }

        /// <summary>
        /// Configure silo to use Cassandra storage as the default grain storage.
        /// </summary>
        public static IServiceCollection AddCassandraGrainStorageAsDefault(
            this IServiceCollection services,
            Action<CassandraStorageOptions> configureOptions,
            IConcurrentGrainStateTypesProvider concurrentGrainStateTypesProvider = null)
        {
            return services.AddCassandraGrainStorage(
                ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME,
                ob => ob.Configure(configureOptions),
                concurrentGrainStateTypesProvider);
        }

        /// <summary>
        /// Configure silo to use Cassandra storage for grain storage.
        /// </summary>
        public static IServiceCollection AddCassandraGrainStorage(
            this IServiceCollection services,
            string name,
            Action<CassandraStorageOptions> configureOptions,
            IConcurrentGrainStateTypesProvider concurrentGrainStateTypesProvider = null)
        {
            return services.AddCassandraGrainStorage(name, ob => ob.Configure(configureOptions), concurrentGrainStateTypesProvider);
        }

        /// <summary>
        /// Configure silo to use Cassandra storage as the default grain storage.
        /// </summary>
        public static IServiceCollection AddCassandraGrainStorageAsDefault(
            this IServiceCollection services,
            Action<OptionsBuilder<CassandraStorageOptions>> configureOptions = null,
            IConcurrentGrainStateTypesProvider concurrentGrainStateTypesProvider = null)
        {
            return services.AddCassandraGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions, concurrentGrainStateTypesProvider);
        }

        /// <summary>
        /// Configure silo to use Cassandra storage for grain storage.
        /// </summary>
        public static IServiceCollection AddCassandraGrainStorage(
            this IServiceCollection services,
            string name,
            Action<OptionsBuilder<CassandraStorageOptions>> configureOptions = null,
            IConcurrentGrainStateTypesProvider concurrentGrainStateTypesProvider = null)
        {
            configureOptions?.Invoke(services.AddOptions<CassandraStorageOptions>(name));

            //services.AddTransient<IConfigurationValidator>(sp => new CassandraStorageOptionsValidator(sp.GetService<IOptionsSnapshot<CassandraStorageOptions>>().Get(name), name));
            services.ConfigureNamedOptionForLogging<CassandraStorageOptions>(name);
            services.TryAddSingleton(sp => sp.GetServiceByName<IGrainStorage>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME));

            return services
                   .AddSingletonNamedService(name, (sp, n) => concurrentGrainStateTypesProvider ?? new NullConcurrentGrainStateTypesProvider())
                   .AddSingletonNamedService(name, CassandraGrainStorageFactory.Create)
                   .AddSingletonNamedService(name, (sp, n) => (ILifecycleParticipant<ISiloLifecycle>)sp.GetRequiredServiceByName<IGrainStorage>(n));
        }
    }
}