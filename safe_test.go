package health

import (
	"testing"

	. "github.com/onsi/gomega"
)

func TestString(t *testing.T) {
	RegisterTestingT(t)
	t.Run("Happy path", func(t *testing.T) {
		b := newBool()

		// default should be "false"
		Expect(b.String()).To(Equal("false"))
		Expect(b.val()).To(BeFalse())

		b.setFalse()
		Expect(b.String()).To(Equal("false"))
		Expect(b.val()).To(BeFalse())

		b.setTrue()
		Expect(b.String()).To(Equal("true"))
		Expect(b.val()).To(BeTrue())

		b.set(true)
		Expect(b.String()).To(Equal("true"))
		Expect(b.val()).To(BeTrue())
	})
}
