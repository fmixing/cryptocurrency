import kotlinx.coroutines.channels.*
import java.lang.IllegalStateException

interface DistributedSystem<ChannelMessage> {
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

typealias processId = Int

typealias money = Int

fun <CM> DistributedSystem<CM>.getChannel(process: processId) = getChannels()[process]
        ?: throw IllegalStateException("Process $process doesn't exist")

fun <CM> DistributedSystem<CM>.getBalance(process: processId) = getBalances()[process]
        ?: throw IllegalStateException("Process $process doesn't exist")

fun <CM> DistributedSystem<CM>.getProcessQuorumSystem(process: processId) = getQuorumSystem()[process]
        ?: throw IllegalStateException("Process $process doesn't exist")