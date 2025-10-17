package dsl

import org.slf4j.LoggerFactory
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path

sealed class DataSource {
    data class FromFile(val path: String) : DataSource()
    data class FromUrl(val url: String) : DataSource()
    data class InMemory(val data: List<String>) : DataSource()
}

interface Pipeline<T> {
    fun execute(): T
}

class MyPipeline<T>(
    private val dataSource: DataSource?,
    private val steps: List<StepOperation>
) : Pipeline<T> {
    private val logger = LoggerFactory.getLogger(javaClass)

    override fun execute(): T {
        var data: Any? = when (dataSource) {
            is DataSource.FromFile -> extractDataFromFile(dataSource)
            is DataSource.FromUrl -> extractDataFromUrl(dataSource)
            is DataSource.InMemory -> dataSource.data
            null -> throw IllegalArgumentException("DataSource not set")
        }

        for (op in steps) {
            logger.info("Executing step: ${op.name}")
            data = op.apply(data)
        }

        @Suppress("UNCHECKED_CAST")
        return data as T
    }

    private fun extractDataFromFile(file: DataSource.FromFile): List<String> {
        return Files.readAllLines(Path.of(file.path))
    }

    private fun extractDataFromUrl(url: DataSource.FromUrl): List<String> {
        return URI(url.url).toURL().readText()
            .lineSequence()
            .toList()
    }
}

interface StepOperation {
    val name: String
    fun apply(input: Any?): Any?
}

private data class StepOperationImpl<I, O>(
    override val name: String,
    val operation: (I) -> O
) : StepOperation {
    @Suppress("UNCHECKED_CAST")
    override fun apply(input: Any?): Any? {
        val typed = input as I
        return operation(typed)
    }
}

interface PipelineBuilder {
    fun source(dataSource: DataSource)
    fun <T, R> step(name: String, operation: (T) -> R)
}

class MyPipelineBuilder : PipelineBuilder {
    private var dataSource: DataSource? = null
    private val steps: MutableList<StepOperation> = mutableListOf()

    override fun source(dataSource: DataSource) {
        this.dataSource = dataSource
    }

    override fun <T, R> step(name: String, operation: (T) -> R) {
        steps += StepOperationImpl<T,R>(name, operation)
    }

    fun <T> build(): Pipeline<T> = MyPipeline(dataSource, steps)
}

fun <T> pipeline(init: PipelineBuilder.() -> Unit): Pipeline<T> {
    val pipelineBuilder = MyPipelineBuilder().apply(init)
    return pipelineBuilder.build()
}