import kotlinx.coroutines.channels.*

interface DistributedSystem {
    fun getProcesses() : Set<processId>

    fun getChannels() : Map<processId, Channel<ChannelMessage>>

    fun getQuorumSystem() : Map<processId, FBQS>

    fun getBalances() : Map<processId, Int>
}

open class ChannelMessage(val process: processId, val message: Message)

class Message(val transactionInfo: TransactionInfo, val deps: Set<TransactionInfo>) {
    val sender = transactionInfo.sender
    val receiver = transactionInfo.receiver
    val transferId = transactionInfo.transferId
    val transferValue = transactionInfo.transferValue
}

class SymmetricDistributedSystem : DistributedSystem {
    private val channels = mapOf(Pair(0, Channel<ChannelMessage>(Channel.UNLIMITED)),
            Pair(1, Channel(Channel.UNLIMITED)))


    override fun getProcesses(): Set<processId> {
        return channels.keys
    }

    override fun getChannels(): Map<processId, Channel<ChannelMessage>> {
        return channels
    }

    override fun getQuorumSystem(): Map<processId, FBQS> {
        return mapOf(Pair(0, SymmetricFBQS()), Pair(1, SymmetricFBQS()))
    }

    override fun getBalances(): Map<processId, Int> {
        return mapOf(Pair(0, 100), Pair(1, 100))
    }

    private class SymmetricFBQS : FBQS {
        override fun hasQuorum(process: processId, processes: Set<processId>): Boolean {
            return processes.contains(0) && processes.contains(1)
        }

        override fun hasBlockingSet(process: processId, processes: Set<processId>): Boolean {
            return processes.contains(0) && process == 1 || processes.contains(1) && process == 0
        }
    }
}

