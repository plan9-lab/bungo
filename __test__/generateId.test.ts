import { Bongo } from '..'
import { it, expect } from 'bun:test'

it('should generate a unique id', (done) => {
	expect(Bongo.generateBongoId()).not.toEqual(Bongo.generateBongoId())
	done()
})

// TODO: проверить что они реально сортируются по алфовиту в нужно порядке
// TODO: перевести на английский комменты
it('should generate 100 unique ids', (done) => {
	const ids = new Array(100).fill(0).map(() => Bongo.generateBongoId())
	const uniqueIds = new Set(ids)
	expect(uniqueIds.size).toEqual(100)
	done()
})