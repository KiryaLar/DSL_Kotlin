package dsl

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class PipelineDslTest {
    @Test
    fun pipeline_with_transforms_types() {
        val pipeline1 = pipeline<Int> {
            source(DataSource.InMemory(listOf("1", "2", "3")))

            step("Parse") { sth: List<String> ->
                sth.map { it.toInt() }
            }

            step("Sum") { sth: List<Int> ->
                sth.sum()
            }
        }.execute()

        assertEquals(6, pipeline1)
    }

    @Test
    fun empty_pipeline() {
        val pipeline2 = pipeline<List<String>> {
            source(DataSource.InMemory(emptyList()))

            step("Process") { sth: List<String> ->
                sth.map { it.uppercase() }
            }
        }.execute()

        assertTrue(pipeline2.isEmpty())
    }

    @Test
    fun pipeline_with_multiple_transforms() {
        val pipeline3 = pipeline<String> {
            source(DataSource.InMemory(listOf("hello", "world")))

            step("Join") { sth: List<String> ->
                sth.joinToString(" ")
            }

            step("Capitalize") { s: String ->
                s.uppercase()
            }
        }.execute()

        assertEquals("HELLO WORLD", pipeline3)
    }
}