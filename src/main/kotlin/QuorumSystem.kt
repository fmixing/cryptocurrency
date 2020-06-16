interface FBQS {
    /**
     * Проверяет, есть ли в множестве процессов [processes] кворум указанного процесса [process]
     */
    fun hasQuorum(process: ProcessId, processes: Set<ProcessId>) : Boolean

    /**
     * Проверяет, есть ли в множестве процессов [processes] блокирующее множество указанного процесса [process]
     */
    fun hasBlockingSet(process: ProcessId, processes: Set<ProcessId>) : Boolean
}