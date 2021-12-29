package mr

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadFile(t *testing.T) {
	content := ReadFile("testInput.txt")
	fmt.Println(content)
	expected := "testline word123\nalice 234 bob\ncharlie"
	assert.Equal(t, expected, content)
}

func TestIHash(t *testing.T) {
	assert.Equal(t, 2, ihash("book")%10)
}

func TestWriteIntermediateDataToFile(t *testing.T) {
	data := []KeyValue{{Key: "k1", Value: "v1"}, {Key: "k2", Value: "v2"}}
	WriteIntermediateDataToFile(data, "temp.json")

	output := ReadJsonData("temp.json")
	assert.ElementsMatch(t, data, output)

	os.Remove("temp.json")
}
