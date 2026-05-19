package org.misterstorm.distributedlock.web.routes

import tools.jackson.databind.ObjectMapper
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.ResultMatcher
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders
import org.springframework.test.web.servlet.result.MockMvcResultMatchers
import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.models.lock.LockCandidate
import java.time.LocalDateTime

class HttpRequest(val mvc: MockMvc, val objectMapper: ObjectMapper) {
    private lateinit var method: String
    private lateinit var url: String
    private var body: String? = null
    private val matchers = mutableListOf<ResultMatcher>()

    fun post(path: String): HttpRequest {
        this.method = "POST"
        this.url = path
        return this
    }

    fun delete(path: String): HttpRequest {
        this.method = "DELETE"
        this.url = path
        return this
    }

    fun put(path: String): HttpRequest {
        this.method = "PUT"
        this.url = path
        return this
    }

    fun get(path: String): HttpRequest {
        this.method = "GET"
        this.url = path
        return this
    }

    fun withBody(body: Any): HttpRequest {
        this.body = objectMapper.writeValueAsString(body)
        return this
    }

    fun expectStatus(status: Int): HttpRequest {
        matchers.add(MockMvcResultMatchers.status().`is`(status))
        return this
    }

    fun expectJsonPath(path: String, value: Any): HttpRequest {
        matchers.add(MockMvcResultMatchers.jsonPath(path).value(value))
        return this
    }

    fun execute() {
        val requestBuilder = when (method) {
            "POST" -> MockMvcRequestBuilders.post(url).contentType(MediaType.APPLICATION_JSON)
            "DELETE" -> MockMvcRequestBuilders.delete(url).contentType(MediaType.APPLICATION_JSON)
            "PUT" -> MockMvcRequestBuilders.put(url).contentType(MediaType.APPLICATION_JSON)
            "GET" -> MockMvcRequestBuilders.get(url)
            else -> throw IllegalArgumentException("Unsupported HTTP method: $method")
        }

        if (body != null) {
            requestBuilder.content(body!!)
        }

        val initialResult = mvc.perform(requestBuilder).andReturn()
        val result = if (initialResult.request.isAsyncStarted) {
            mvc.perform(MockMvcRequestBuilders.asyncDispatch(initialResult)).andReturn()
        } else {
            initialResult
        }
        for (matcher in matchers) {
            matcher.match(result)
        }
    }
}

fun lockCandidate(
    key: String = "test-key",
    clientId: String = "test-client-id"
) = LockCandidate(key = key, clientId = clientId)

fun lock(
    key: String = "test-key",
    lockOwner: String = "test-client-id",
    expirationTime: LocalDateTime = LocalDateTime.now().plusSeconds(120)
) = Lock(key = key, lockOwner = lockOwner, expirationTime = expirationTime)

fun MockMvc.http(objectMapper: ObjectMapper) = HttpRequest(this, objectMapper)



