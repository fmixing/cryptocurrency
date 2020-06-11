import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.scheduling.*
import kotlin.reflect.*

@InternalCoroutinesApi
private val context = ExperimentalCoroutineDispatcher(corePoolSize = 2, maxPoolSize = 2)

class Broadcast(private val channels: Map<processId, Channel<ChannelMessage>>,
                private val processChannel: Channel<ChannelMessage>,
                private val processId: processId,
                private val quorumSystem: FBQS,
                private val deliver: KSuspendFunction1<Message, Unit>) {
    private val echoed : MutableMap<BroadcastId, Boolean> = HashMap()
    private val readied : MutableMap<BroadcastId, Boolean> = HashMap()
    private val delivered : MutableMap<BroadcastId, Boolean> = HashMap()
    private val receivedEcho : MutableMap<BroadcastId, MutableMap<Message, MutableSet<processId>>> = HashMap()
    private val receivedReady : MutableMap<BroadcastId, MutableMap<Message, MutableSet<processId>>> = HashMap()
    @ExperimentalCoroutinesApi
    @InternalCoroutinesApi
    private val runCoroutine = CoroutineScope(context).launch { receive() }

    suspend fun broadcast(m: Message) {
        channels.values.forEach { it.send(Bcast(processId, m)) }
    }

    private suspend fun receive() {
        while (true) {
            val message = processChannel.receive()
            process(message)
        }
    }

    private suspend fun process(cm: ChannelMessage) {
        val broadcastId = BroadcastId(cm.message.sender, cm.message.transferId)
        when (cm) {
            is Bcast -> processBcast(broadcastId, cm)
            is Echo -> processEcho(broadcastId, cm)
            is Ready -> processReady(broadcastId, cm)
        }
    }

    private suspend fun processBcast(id: BroadcastId, m: ChannelMessage) {
        if (!echoed.getOrElse(id) { false }) {
            channels.values.forEach { it.send(Echo(processId, m.message)) }
            echoed[id] = true
        }
    }

    private suspend fun processEcho(id: BroadcastId, m: ChannelMessage) {
        if (readied.getOrElse(id) { false }) return

        val echoedProcess: MutableSet<processId> = receivedEcho.getOrPut(id){ HashMap() }.getOrPut(m.message){ HashSet() }
        echoedProcess += m.process
        val hasQuorum = quorumSystem.hasQuorum(processId, echoedProcess)
        if (hasQuorum) {
            channels.values.forEach { it.send(Ready(processId, m.message)) }
            readied[id] = true
        }
    }

    private suspend fun processReady(id: BroadcastId, m: ChannelMessage) {
        if (delivered.getOrElse(id) { false }) return

        val readiedProcess: MutableSet<processId> = receivedReady.getOrPut(id){ HashMap() }.getOrPut(m.message){ HashSet() }
        readiedProcess += m.process
        val hasQuorum = quorumSystem.hasQuorum(processId, readiedProcess)
        if (hasQuorum) {
            delivered[id] = true
            deliver.invoke(m.message)
            return
        }

        val hasBlockingSet = quorumSystem.hasBlockingSet(processId, readiedProcess)
        if (hasBlockingSet && !readied.getOrElse(id) { false }) {
            channels.values.forEach { it.send(Ready(processId, m.message)) }
            readied[id] = true
        }
    }
}

private class BroadcastId(private val sender: processId, private val transferId: Int) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as BroadcastId

        if (sender != other.sender) return false
        if (transferId != other.transferId) return false

        return true
    }

    override fun hashCode(): Int {
        var result = sender
        result = 31 * result + transferId
        return result
    }
}

private class Bcast(process: processId, m: Message): ChannelMessage(process, m)

private class Echo(process: processId, m: Message): ChannelMessage(process, m)

private class Ready(process: processId, m: Message): ChannelMessage(process, m)

//BroadCast<BId, Message>