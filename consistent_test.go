package consistenthash_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"consistenthash"
)

var (
	node1id = "node1"
	node2id = "node2"
	node3id = "node3"
)

var _ = Describe("Adding nodes", func() {
	Context("With one node", Ordered, func() {
		var r *consistenthash.Ring

		r = consistenthash.NewRing()

		BeforeAll(func() {
			r.AddNode("")
		})
		It("Should add one node", func() {
			Expect(r.Nodes.Len()).To(Equal(1))
		})
		It("Should generate a valid node hash", func() {
			Expect(r.Nodes[0].HashId).To(Equal(uint32(0)))
		})
	})
	Context("With multiple nodes", Ordered, func() {
		var r *consistenthash.Ring

		r = consistenthash.NewRing()

		BeforeAll(func() {
			r.AddNode(node1id)
			r.AddNode(node2id)
			r.AddNode(node3id)
		})
		It("Should add nodes", func() {
			Expect(r.Nodes.Len()).To(Equal(3))
		})
		It("Should sort nodes by node hash", func() {
			Expect(r.Nodes[0].HashId).To(BeNumerically("<", r.Nodes[1].HashId))
			Expect(r.Nodes[1].HashId).To(BeNumerically("<", r.Nodes[2].HashId))

			Expect(r.Nodes[0].Id).To(Equal(node2id))
			Expect(r.Nodes[1].Id).To(Equal(node3id))
			Expect(r.Nodes[2].Id).To(Equal(node1id))
		})

	})
})

var _ = Describe("Removing nodes", func() {
	Context("Given ring with nodes", func() {
		var r *consistenthash.Ring

		r = consistenthash.NewRing()
		r.AddNode(node1id)
		r.AddNode(node2id)
		r.AddNode(node3id)

		When("The node exists", Ordered, func() {
			var err error
			BeforeAll(func() {
				err = r.RemoveNode(node2id)
			})
			It("Should not return error", func() {
				Expect(err).To(BeNil())
			})
			It("Should remove the node", func() {
				Expect(r.Nodes.Len()).To(Equal(2))
				Expect(r.Nodes[0].Id).To(Equal(node3id))
				Expect(r.Nodes[1].Id).To(Equal(node1id))
			})
		})
		When("The node does not exist", Ordered, func() {
			var err error
			BeforeAll(func() {
				err = r.RemoveNode("nonexistent")
			})
			It("Should return error", func() {
				Expect(err).ToNot(BeNil())
			})
		})
	})
})

var _ = Describe("Getting node", func() {
	Context("Given ring with 1 node", Ordered, func() {
		var (
			r                              *consistenthash.Ring
			key1, key2, key1Node, key2Node string
		)

		r = consistenthash.NewRing()
		r.AddNode(node1id)

		key1 = "key1"
		key2 = "key2"

		BeforeAll(func() {
			key1Node = r.Get(key1)
			key2Node = r.Get(key2)
		})
		It("Should always return that node", func() {
			Expect(key1Node).To(Equal(node1id))
			Expect(key2Node).To(Equal(node1id))
		})
	})
	Context("Given ring with multiple nodes", Ordered, func() {
		var (
			r              *consistenthash.Ring
			key1, key1Node string
			key2, key2Node string
			key3, key3Node string
			key4, key4Node string
		)

		r = consistenthash.NewRing()
		r.AddNode(node1id)
		r.AddNode(node2id)
		r.AddNode(node3id)

		key1 = "justa"
		key2 = "justb"
		key3 = "justc"
		key4 = "justd"

		BeforeAll(func() {
			key1Node = r.Get(key1)
			key2Node = r.Get(key2)
			key3Node = r.Get(key3)
			key4Node = r.Get(key4)
		})
		It("Should return the closest node", func() {
			Expect(key1Node).To(Equal(node1id))
			Expect(key2Node).To(Equal(node2id))
			Expect(key3Node).To(Equal(node1id))
			Expect(key4Node).To(Equal(node3id))
		})

		Context("Removing a node", Ordered, func() {
			BeforeAll(func() {
				r.RemoveNode(node3id)

				key1Node = r.Get(key1)
				key2Node = r.Get(key2)
				key3Node = r.Get(key3)
				key4Node = r.Get(key4)
			})

			It("The keys of the remaining nodes should not be re-assigned", func() {
				Expect(key1Node).To(Equal(node1id))
				Expect(key2Node).To(Equal(node2id))
				Expect(key3Node).To(Equal(node1id))
			})
			It("The keys of the removed node should be reassigned to the next closest node", func() {
				Expect(key4Node).To(Equal(node1id))
			})
		})
	})
})
