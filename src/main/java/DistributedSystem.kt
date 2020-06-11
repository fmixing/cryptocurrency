import kotlinx.coroutines.channels.*

interface DistributedSystem {
    /**
     * Получение множества прооцессов распределенной системы.
     */
    fun getProcesses() : Set<processId>

    /**
     * "Надежные" каналы связи, по которым процессы могут слать друг другу сообщения.
     */
    fun getChannels() : Map<processId, Channel<ChannelMessage>>

    /**
     * Асимметричная система кворумов, в ней для каждого процесса задается своя система кворумов.
     */
    fun getQuorumSystem() : Map<processId, FBQS>

    /**
     * Исходные балансы процессов.
     */
    fun getBalances() : Map<processId, money>
}

open class ChannelMessage(val process: processId, val message: Message)

class Message(val transactionInfo: TransactionInfo, val deps: Set<TransactionInfo>) {
    val sender = transactionInfo.sender
    val receiver = transactionInfo.receiver
    val transferId = transactionInfo.transferId
    val transferValue = transactionInfo.transferValue
}

/**
 * Симметричная система кворумов, в которой кворумы описаны множествами процессов, построенными на кворумных слайсах.
 */
class SymmetricDistributedSystem : DistributedSystem {
    private val channels = HashMap<processId, Channel<ChannelMessage>>()
    private val qs: MutableMap<processId, FBQS> = HashMap()
    private val balances: MutableMap<processId, money> = HashMap()

    init {
        repeat(4) {
            channels[it] = Channel(Channel.UNLIMITED)
        }
        repeat(4) {
            qs[it] = SymmetricFBQS()
        }
        repeat(4) {
            balances[it] = 100
        }
    }

    override fun getProcesses(): Set<processId> {
        return channels.keys
    }

    override fun getChannels(): Map<processId, Channel<ChannelMessage>> {
        return channels
    }

    override fun getQuorumSystem(): Map<processId, FBQS> {
        return qs
    }

    override fun getBalances(): Map<processId, Int> {
        return balances
    }

    private inner class SymmetricFBQS : FBQS {
        // система кворумов, построенная по кворумным слайсам:
        // {{0, 1}, {0, 1, 2}, {0, 2, 3}, {0, 1, 2, 3}}
        private val q1 = setOf(0, 1)
        private val q2 = setOf(0, 1, 2)
        private val q3 = setOf(0, 2, 3)
        private val q4 = setOf(0, 1, 2, 3)

        override fun hasQuorum(process: processId, processes: Set<processId>): Boolean {
            return when (process) {
                0 -> processes.containsAll(q1) || processes.containsAll(q2) || processes.containsAll(q3) || processes.containsAll(q4)
                1 -> processes.containsAll(q1) || processes.containsAll(q2) || processes.containsAll(q4)
                2 -> processes.containsAll(q2) || processes.containsAll(q3) || processes.containsAll(q4)
                3 -> processes.containsAll(q3) || processes.containsAll(q4)
                else -> false
            }
        }

        override fun hasBlockingSet(process: processId, processes: Set<processId>): Boolean {
            // Кворумные слайсы:
            // S(0) = {{0, 1}, {0, 3}}
            // S(1) = {{0, 1}}
            // S(2) = {{0, 2}}
            // S(3) = {{2, 3}}
            return when (process) {
                0 -> processes.contains(1) && processes.contains(3)
                1 -> processes.contains(0)
                2 -> processes.contains(0)
                3 -> processes.contains(2)
                else -> false
            }
        }
    }
}

/**
 * "Мажоритарная" система кворумов: в ней кворумом считается больше половины процессов.
 */
class MajoritySymmetricDistributedSystem : DistributedSystem {
    private val numberOfProcesses = 20
    private val channels: MutableMap<processId, Channel<ChannelMessage>> = HashMap()
    private val qs: MutableMap<processId, FBQS> = HashMap()
    private val balances: MutableMap<processId, money> = HashMap()

    init {
        repeat(numberOfProcesses) {
            channels[it] = Channel(Channel.UNLIMITED)
        }
        repeat(numberOfProcesses) {
            qs[it] = SymmetricFBQS()
        }
        repeat(numberOfProcesses) {
            balances[it] = 100
        }
    }

    override fun getProcesses(): Set<processId> = channels.keys

    override fun getChannels(): Map<processId, Channel<ChannelMessage>> = channels

    override fun getQuorumSystem(): Map<processId, FBQS> = qs

    override fun getBalances(): Map<processId, money> = balances

    private inner class SymmetricFBQS : FBQS {
        override fun hasQuorum(process: processId, processes: Set<processId>): Boolean {
            return processes.size > numberOfProcesses / 2
        }

        override fun hasBlockingSet(process: processId, processes: Set<processId>): Boolean {
            return processes.size > numberOfProcesses / 2
        }
    }
}
