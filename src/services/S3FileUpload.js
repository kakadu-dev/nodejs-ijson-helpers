const AWS   = require('aws-sdk');
const axios = require('axios');
const _     = require('lodash');

/**
 * AWS S3 File Upload
 *
 * Works with base64 only
 */
class S3FileUpload
{
	/**
	 * @type {S3}
	 *
	 * @private
	 */
	_s3 = null;

	/**
	 * @type {Object}
	 *
	 * @private
	 */
	_awsConfig = {};

	/**
	 * @constructor
	 *
	 * @param {Object} awsConfig
	 */
	constructor(awsConfig)
	{
		this._awsConfig = awsConfig;

		AWS.config.update({
			accessKeyId:     awsConfig?.accessKeyId ?? null,
			secretAccessKey: awsConfig?.accessSecretKey ?? null,
			region:          awsConfig?.region ?? null,
		});

		this._s3 = new AWS.S3({ params: { Bucket: awsConfig?.bucketName ?? null } });
	}

	/**
	 * Get destination file name
	 *
	 * @param {string} fileName
	 * @param {string}extension
	 *
	 * @return {string}
	 * @private
	 */
	_getDestinationFileName(fileName, extension)
	{
		if (fileName) {
			return fileName.includes(extension) ? fileName : `${fileName}.${extension}`;
		}

		return `${+(new Date())}_${_.random(10000, 99999)}.${extension}`;
	}

	/**
	 * Get file from url
	 *
	 * @param {string} url
	 *
	 * @return {Promise<{name: *, type: string, base64: string}>}
	 * @private
	 */
	async _getFileFromUrl(url)
	{
		const response = await axios.get(url, { responseType: 'arraybuffer' });

		return {
			name:   url.replace(/^.*[\\\/]/, ''),
			type:   response?.headers?.['content-type'] ?? null,
			base64: Buffer.from(response?.data ?? '').toString('base64'),
		};
	}

	/**
	 * Save filte to S3
	 *
	 * @param {Object} fileObj
	 * @param {string} destPath without begin and end slashes
	 * @param {string} fileName if not passed, generate automatically
	 *
	 * @return {Promise<string|boolean>}
	 */
	async save(fileObj, destPath = '', fileName = null)
	{
		if (typeof fileObj === 'string') {
			if (fileObj.length === 0) {
				throw new Error('URL строка не может быть пустой.');
			}

			fileObj = await this._getFileFromUrl(fileObj);
		}

		const { name, type, base64 } = fileObj || {};

		if (!type) {
			throw new Error(`Тип файла не определен: ${type}`);
		}

		if (!base64) {
			throw new Error('Файл не найден.');
		}

		const extension = name.split('.')?.[1] ?? null;

		if (!extension) {
			throw new Error(`Расширение файла не установлено: ${name}`);
		}

		// Remove begin and end slashes
		const destinationPath = (destPath || '').replace(/(^\/+|\/+$)/g, '');
		const destinationName = this._getDestinationFileName(fileName, extension);

		const result = await this._s3.putObject({
			Body:            Buffer.from(base64.replace(/^data:.+;base64,/, ''), 'base64'),
			Key:             [destinationPath, destinationName].filter(Boolean).join('/'),
			ContentEncoding: 'base64',
			ContentType:     type,
		}).promise();

		return (result?.ETag?.length ?? 0) > 0 ? destinationName : false;
	}

	/**
	 * Delete file from S3
	 *
	 * @param {string} filePath path to file
	 *
	 * @return {Promise<boolean>}
	 */
	async remove(filePath)
	{
		if (!filePath) {
			throw new Error('Не передан путь к файлу.');
		}

		const result = await this._s3.deleteObject({
			Key: filePath,
		}).promise();

		return result?.DeleteMarker ?? false;
	}

	/**
	 * Get absolute file url
	 *
	 * @param {string} filePath
	 *
	 * @return {string}
	 */
	getAbsoluteFileUrl(filePath)
	{
		const domains = (this._awsConfig?.bucketCdn ?? []) || [];
		// Get random cdn domain. Remove end slash
		const domain  = domains?.[Math.floor(Math.random() * domains.length)]?.replace(/\/+$/, '') ?? '';

		return `${domain}/${filePath.replace(/^\/+/, '')}`;
	}
}

module.exports = S3FileUpload;
