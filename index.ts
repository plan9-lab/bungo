/**
 * @packageDocumentation
 * @module Bungo
 */
import { Database } from 'bun:sqlite'
import fs, { mkdirSync } from 'node:fs'
import { join, parse } from 'node:path'

const fieldTypes: { [key: string]: Record<string, BongoField> } = {}

const defaultFieldDescription = {
	type: 'TEXT',
	allowNull: true,
	unique: false,
	name: ''
} as any

export function BongoField(options: { type?: 'TEXT' | 'NUMBER' | 'REAL' | 'BLOB' | 'NULL', allowNull?: boolean, unique?: boolean, name?: string } = {
	type: 'TEXT',
	allowNull: true,
	unique: false
}) {
	return (target: any, key: any) => {
		options = {
			...defaultFieldDescription,
			...options
		}

		const targeName = target.constructor.name

		fieldTypes[targeName] = fieldTypes[targeName] ?? {}
		fieldTypes[targeName][key] = fieldTypes[targeName][key] ?? {
			...options,
			name: key
		}
	}
}

export interface IBongoDoc {
	_id?: string | any
	key?: string | any
	//_ct: number | any
	[key: string]: any
}

export class BongoDoc implements IBongoDoc {
	@BongoField() _id? = ''

	constructor(data: Partial<IBongoDoc> = {}) {
		data._id ??= generateBongoId()
		Object.assign(this, data)
	}
}

export type BongoField = {
	name: string
	type: 'TEXT' | 'INTEGER' | 'REAL' | 'BLOB' | any
	allowNull: boolean
	unique: boolean
}

type BongoQueryOptions = {
	sort?: Record<string, number>
	limit?: number
	offset?: number
}

// utils
function objToSqlWhereClause(obj: Record<string, any>) {
	return Object.keys(obj).map(key => `${key} = ?`).join(' AND ')
}

// string collision safe lexically sortable _id
generateBongoId.counter = Math.floor(Math.random() * (1 << 24)) // 3-byte counter

function generateBongoId(): string {
	const timestamp = Math.floor(Date.now() / 1000).toString(16) // 4-byte timestamp
	const randomValue = Math.floor(Math.random() * (1 << 32)).toString(16).padStart(8, '0') // 5-byte random value
	const sequence = (generateBongoId.counter++ % (1 << 24)).toString(16).padStart(6, '0') // 3-byte counter

	return timestamp + randomValue + sequence
}

export class Bongo {
	db: Database

	path: string

	bongo: Bongo // this is for easear migration from Bong to BongoService

	// Utils
	static join = join

	collectionsToDocClasses: Record<string, BongoDoc> = {}

	constructor(path: string) {
		if (!path.endsWith('.sqlite')) {
			path += '.sqlite'
		}

		//path = join(process.cwd(), path)
		const parsedPath = parse(path)

		mkdirSync(parsedPath.dir, { recursive: true })
		try {
			this.db = new Database(path)
			this.path = path
			this.bongo = this
		} catch (error: any) {
			console.error('bongo error:', 'uable to open path', path)
			throw error
		}
	}

	static generateBongoId = generateBongoId

	async createCollectionRaw(collection: string, fields: BongoField[]) {
		const idField = '_id TEXT NOT NULL PRIMARY KEY' // lexically sortable _id
		const ctField = '_ct INTEGER NOT NULL' // creation time
		//const keyField = 'key TEXT NOT NULL' // react key
		const sqlQuery = `CREATE TABLE IF NOT EXISTS ${collection} (${idField})`

		try {
			this.db.query(sqlQuery).run()
			for (const field of fields) {
				try {
					await this.addField(collection, field.name, field.type, field.allowNull)
				} catch (error: any) {
					console.log(collection, field)
					throw error
				}
				if (field.unique) {
					this.ensureIndex(collection, [field.name], true)
				}
			}

			const result = this.db.query(sqlQuery).run()

			return result
		} catch (error: any) {
			console.log(collection)
			throw error
		}
	}

	async createCollection(collection: string, DocClass: any = BongoDoc) {
		const fields = Object.values(fieldTypes[DocClass.name])

		await this.createCollectionRaw(collection, fields)
		this.collectionsToDocClasses[collection] = DocClass
	}

	async addField(collection: string, field: string, type: any, allowNull = false) {
		try {
			const sqlQuery = `ALTER TABLE ${collection} ADD COLUMN ${field} ${type} ${allowNull ? 'NULL' : 'NOT NULL'}`

			return this.db.query(sqlQuery).run()
		} catch (error: any) {
			if (!error.message.includes('duplicate column name')) {
				console.log('addField error:', collection, field, type, allowNull)
				throw error
			}
		}
	}

	async alterCollection(collection: string, DocClass: any) {
		const tmpCollection = `${collection}_tmp`

		this.createCollection(tmpCollection, DocClass)
	}

	async ensureIndex(collection: string, fields: string[], unique: boolean = false): Promise<void> {
		const fieldsSql = fields.join(', ')
		const sqlQuery = `CREATE ${unique ? 'UNIQUE' : ''} INDEX IF NOT EXISTS idx_${collection}_${fields.join('_')} ON ${collection} (${fieldsSql})`

		this.db.query(sqlQuery).run()
	}

	async find<T extends BongoDoc>(collection: string, query: Record<string, any> = {}, options: BongoQueryOptions = {}): Promise<T[]> {
		let sqlQuery = `SELECT * FROM ${collection}`

		if (Object.keys(query).length > 0) {
			const whereClause = Object.entries(query).map(([
				key,
				value
			]) => `${key}='${value}'`).join(' AND ')

			sqlQuery += ` WHERE ${whereClause}`
		}

		if (options.sort) {
			const sortClause = Object.entries(options.sort).map(([
				field,
				order
			]) => `${field} ${order > 0 ? 'ASC' : 'DESC'}`).join(', ')

			sqlQuery += ` ORDER BY ${sortClause}`
		}

		if (options.limit) {
			sqlQuery += ` LIMIT ${options.limit}`
		}

		if (options.offset) {
			sqlQuery += ` OFFSET ${options.offset}`
		}

		let data = []

		try {
			data = this.db.query(sqlQuery).all() as any
		} catch (error) {
			console.log('collection', collection)
			throw error
		}

		return data.map((doc: any) => {
			doc.key = doc._id.length > 4 ? doc._id.slice(-4) : doc._id
			return doc
		}) as T[]
	}

	// findOne returning result from find or null
	async findOne<T extends BongoDoc>(collection: string, query: Record<string, any> = {}, options: BongoQueryOptions = {}): Promise<T | null> {
		const results = await this.find<T>(collection, query, options)

		return results[0] ?? null
	}

	async insert<T extends BongoDoc>(collection: string, data: T | any): Promise<{ insertedId: string }> {
		data._id ??= generateBongoId()

		//data._ct = Date.now()
		//data.key ??= data._id // key for react
		const keys = Object.keys(data).join(', ')
		const values = Object.values(data).map(value => `'${value}'`).join(', ')
		const sqlQuery = `INSERT INTO ${collection} (${keys}) VALUES (${values})`

		this.db.query(sqlQuery).run()
		return { insertedId: data._id }
	}

	async insertOne<T extends BongoDoc>(collection: string, data: T | any): Promise<{ insertedId: string }> {
		return this.insert(collection, data)
	}

	async update<T extends BongoDoc>(collection: string, query: Record<string, any>, data: T | any): Promise<void> {
		const docs = await this.find(collection, query)

		for (const doc of docs) {
			const updates = Object.keys(data)
				.map(key => `${key}='${data[key]}'`)
				.join(', ')
			const sqlQuery = `UPDATE ${collection} SET ${updates} WHERE _id='${doc._id}'`

			try {
				this.db.query(sqlQuery).run()
			} catch (error: any) {
				console.error('update error:', collection, query, data)
				throw error
			}
		}
	}

	async deleteMany(collection: string, query: Record<string, any> = {}, options: BongoQueryOptions = {}): Promise<{ deletedCount: number }> {
		try {
			const docs = await this.find(collection, query, options)

			for (const doc of docs) {
				const sqlQuery = `DELETE FROM ${collection} WHERE _id='${doc._id}'`

				this.db.query(sqlQuery).run()
			}
			return { 'deletedCount': docs.length }
		} catch (error: any) {
			if (error.message.includes('no such table')) {
				console.warn('bongo.deleteMany', error.message)
				return { 'deletedCount': 0 }
			}
		}
		// const docs = await this.find(collection, query, options)
		// for (const doc of docs) {
		// 	const sqlQuery = `DELETE FROM ${collection} WHERE _id='${doc._id}'`
		// 	this.db.query(sqlQuery).run()
		// }
		// return {
		// 	'deletedCount': docs.length
		// }
	}

	async deleteOne(collection: string, query: Record<string, any> = {}, options: BongoQueryOptions = {}): Promise<any> {
		const doc = await this.findOne(collection, query, options)

		if (!doc) {
			return { 'deletedCount': 0 }
		}

		const sqlQuery = `DELETE FROM ${collection} WHERE _id='${doc._id}' LIMIT 1`

		this.db.query(sqlQuery).run()
		return { 'deletedCount': 1 }
	}

	async dropDatabase() {
		this.db.close()
		await fs.promises.unlink(this.path)
		this.db = new Database(this.path)
	}

	async truncateAll() {
		const tables: any = this.db.query('SELECT name FROM sqlite_master WHERE type=\'table\'').all()

		for (const table of tables) {
			this.db.query(`DELETE FROM ${table.name}`).run()
		}
	}
}

export class BongoService {
	bongo: Bongo

	constructor(path: string) {
		this.bongo = new Bongo(path)
	}
}
