const dns = require('dns');
const _   = require('lodash');

/**
 * BaseConnection class
 */
class BaseConnection
{
	/**
	 * @type {Connection}
	 *
	 * @private
	 */
	static instance = null;

	/**
	 * @type {null}
	 *
	 * @private
	 */
	db = null;

	/**
	 * @type {Array}
	 *
	 * @private
	 */
	models = {};

	/**
	 * @type {Object}
	 */
	config = {};

	/**
	 * Prepare db
	 *
	 * @param {Array.<Object>} models
	 * @param {Object} config
	 * @param {Object} modelConfig
	 *
	 * @return {Promise<{db: sequelize.Sequelize, insertDefaults: insertDefaults}|boolean>}
	 */
	static async prepare(models, config = {}, modelConfig = {}) {
		if (this.instance !== null) {
			return false;
		}

		const handledConfig = await BaseConnection.handleSRV(config);
		const connection    = this.getInstance(handledConfig);

		await connection.checkDatabaseExist();

		const associations   = [];
		const insertDefaults = [];

		// Create models/tables
		Object.entries(models).forEach(([name, initModel]) => {
			const model = initModel(connection.getDb(), connection.getSequelize(), modelConfig);

			connection.addModel(name, model);

			if (model.associate) {
				associations.push(model.associate);
			}

			if (model.insertDefault) {
				insertDefaults.push(model.insertDefault);
			}
		});

		// Create relations
		for (const asoc of associations) {
			await asoc(connection.getModels());
		}

		return {
			db:             connection.getDb(),
			insertDefaults: async (...params) => {
				for (const insert of insertDefaults) {
					try {
						await insert(connection.getModels(), ...params);
					} catch (e) {
						// ignore
					}
				}
			},
		};
	}

	/**
	 * Handle host srv record
	 *
	 * @param {Object} config
	 *
	 * @return {Promise<Object>}
	 */
	static async handleSRV(config) {
		const { host, port, slaveHost, ...newConfig } = config;

		const replication = {
			read:  [],
			write: null,
		};

		if (slaveHost) {
			// Simple read replicas host(s)
			if (!slaveHost.endsWith('.srv')) {
				slaveHost.split(',').forEach(h => {
					const [
							  replicaCredentials,
							  replicaHostAndPort,
						  ] = h.split('@');
					const [
							  replicaHost,
							  replicaPort,
						  ] = (replicaHostAndPort || replicaCredentials).split(':');
					const [
							  replicaUsername,
							  replicaPassword,
						  ] = (replicaCredentials || '').split(':');

					replication.read.push({
						host: replicaHost,
						...(replicaPort ? { port: replicaPort } : {}),
						...(replicaUsername ? { username: replicaUsername } : {}),
						...(replicaPassword ? { password: replicaPassword } : {}),
					});
				});
			} else {
				// Add replicas from srv record
				const slaves = await BaseConnection._resolveSrv(slaveHost);
				slaves.forEach(address => {
					replication.read.push({
						host: address.name,
						port: address.port,
					});
				});
			}
		}

		if (!host.endsWith('.srv')) {
			if (!slaveHost) {
				return config;
			}

			replication.write = {
				host,
				port,
			};
			return {
				...newConfig,
				replication,
			};
		}

		const masters = await BaseConnection._resolveSrv(host);
		const master  = masters.shift();

		replication.write = {
			host: master.name,
			port: master.port,
		};

		return {
			...newConfig,
			replication,
		};
	}

	/**
	 * Resolve SRV records
	 *
	 * @param {string} srv
	 *
	 * @return {Promise<[]>}
	 * @private
	 */
	static _resolveSrv(srv) {
		if (!srv?.length) {
			return [];
		}

		return new Promise((resolve, reject) => {
			dns.resolveSrv(srv, (err, addresses) => {
				if (err) {
					return reject(err);
				}

				resolve(_.sortBy(addresses, ['priority', 'weight']));
			});
		});
	}

	/**
	 * Get connection instance
	 *
	 * @param {Object} config
	 *
	 * @return {Connection}
	 */
	static getInstance(config = {}) {
		if (this.instance === null) {
			this.instance = new this();

			// Normalize table prefix name
			if (config?.define?.schema) {
				config.define.schema = config.define.schema.replace(/-/g, '_');
			}

			this.instance.config = config;
			this.instance.createDbConnection();
		}

		return this.instance;
	}

	/**
	 * Create db connection
	 *
	 * @param {Object} dbConfig
	 *
	 * @return {undefined}
	 */
	createDbConnection(dbConfig) {
		this.db = {};
	}

	/**
	 * Get db connection
	 *
	 * @return {Sequelize}
	 */
	getDb() {
		return this.db;
	}

	/**
	 * Get Sequelize
	 *
	 * @return {Sequelize}
	 */
	getSequelize() {
		return {};
	}

	/**
	 * Add db model
	 *
	 * @param {string} name
	 * @param {sequelize.Model} model
	 *
	 * @return {Connection}
	 */
	addModel(name, model) {
		this.models[name] = model;

		return this;
	}

	/**
	 * Get models
	 *
	 * @return {SequelizeDbModels}
	 */
	getModels() {
		return this.models;
	}

	/**
	 * Create database if not exist
	 *
	 * @return {Promise<void>}
	 */
	async checkDatabaseExist() {
		return true;
	}
}

module.exports = BaseConnection;
