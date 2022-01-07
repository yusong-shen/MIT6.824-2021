package mr

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadFile(t *testing.T) {
	content, err := readFile("testInput.txt")
	assert.NoError(t, err)

	fmt.Println(content)
	expected := "testline word123\nalice 234 bob\ncharlie"
	assert.Equal(t, expected, content)
}

func TestIHash(t *testing.T) {
	assert.Equal(t, 2, ihash("book")%10)
}

func TestWriteIntermediateDataToFile(t *testing.T) {
	data := []KeyValue{{Key: "k1", Value: "v1"}, {Key: "k2", Value: "v2"}}
	filename := writeIntermediateDataToFile(data, 2)

	output, err := readJsonData(filename)
	assert.NoError(t, err)
	assert.ElementsMatch(t, data, output)

	os.Remove(filename)
}

func TestAssignKvToReducer(t *testing.T) {
	data := []KeyValue{{Key: "k1", Value: "v1"}, {Key: "k2", Value: "v2"}, {Key: "k1", Value: "v3"}}
	outputDataMap := make(map[int][]KeyValue)
	assignKvToReducer(data, outputDataMap, 2)
	fmt.Println(outputDataMap)
	assert.Equal(t, 2, len(outputDataMap))
	assert.ElementsMatch(t, []KeyValue{{Key: "k2", Value: "v2"}}, outputDataMap[0])
	assert.ElementsMatch(t, []KeyValue{{Key: "k1", Value: "v1"}, {Key: "k1", Value: "v3"}}, outputDataMap[1])

}

func TestReadIntermediateFiles(t *testing.T) {
	filename := "test-intermediate-%v.json"

	mergedData := readIntermediateFiles([]string{fmt.Sprintf(filename, 1), fmt.Sprintf(filename, 2)})
	fmt.Println(mergedData)
	assert.Equal(t, 15, len(mergedData))
	assert.Equal(t, KeyValue{Key: "Being", Value: "1"}, mergedData[0])
	assert.Equal(t, KeyValue{Key: "immoral", Value: "1"}, mergedData[14])

}

func TestSortByKey(t *testing.T) {
	data := []KeyValue{{Key: "k1", Value: "v1"}, {Key: "k2", Value: "v2"}, {Key: "k1", Value: "v3"}}
	sort.Sort(ByKey(data))
	expected := []KeyValue{{Key: "k1", Value: "v1"}, {Key: "k1", Value: "v3"}, {Key: "k2", Value: "v2"}}
	assert.Equal(t, expected, data)
}

func TestApplyReducefAndWriteOuputfile(t *testing.T) {
	reducef := func(key string, values []string) string {
		// return the number of occurrences of this word.
		return strconv.Itoa(len(values))
	}
	data := []KeyValue{{Key: "k1", Value: "v1"}, {Key: "k1", Value: "v3"}, {Key: "k2", Value: "v2"}}

	filename := "testOuput.txt"
	applyReducefAndWriteOutputfile(reducef, data, filename)

	content, err := readFile(filename)
	assert.NoError(t, err)
	expected := "k1 2\nk2 1\n"
	assert.Equal(t, expected, content)
}
