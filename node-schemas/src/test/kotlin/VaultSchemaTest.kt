package net.corda.node.services.vault.schemas

import io.requery.Persistable
import io.requery.TransactionIsolation
import io.requery.kotlin.eq
import io.requery.kotlin.invoke
import io.requery.sql.KotlinConfiguration
import io.requery.sql.KotlinEntityDataStore
import io.requery.sql.SchemaModifier
import io.requery.sql.TableCreationMode
import junit.framework.Assert.assertEquals
import net.corda.core.contracts.*
import net.corda.core.crypto.*
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import net.corda.core.transactions.LedgerTransaction
import net.corda.core.utilities.DUMMY_NOTARY
import net.corda.core.utilities.DUMMY_NOTARY_KEY
import org.h2.jdbcx.JdbcDataSource
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import java.security.KeyPair
import java.time.Instant
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

// Re-defined from test-utils module (which currently depdens and forces re-compile of finance, core, node modules)
val ALICE_KEY: KeyPair by lazy { generateKeyPair() }
val ALICE_PUBKEY: CompositeKey get() = ALICE_KEY.public.composite
val ALICE: Party get() = Party("Alice", ALICE_PUBKEY)
val BOB_KEY: KeyPair by lazy { generateKeyPair() }
val BOB_PUBKEY: CompositeKey get() = BOB_KEY.public.composite
val BOB: Party get() = Party("Bob", BOB_PUBKEY)
val CHARLIE_KEY: KeyPair by lazy { generateKeyPair() }
val CHARLIE_PUBKEY: CompositeKey get() = CHARLIE_KEY.public.composite
val CHARLIE: Party get() = Party("Charlie", CHARLIE_PUBKEY)

class VaultSchemaTest {

    var instance : KotlinEntityDataStore<Persistable>? = null
    val data : KotlinEntityDataStore<Persistable> get() = instance!!

    var transaction : LedgerTransaction? = null

    @Before
    fun setup() {
        val dataSource = JdbcDataSource()
        dataSource.setUrl("jdbc:h2:~/testh2")
        dataSource.user = "sa"
        dataSource.password = "sa"

        val configuration = KotlinConfiguration(dataSource = dataSource, model = Models.DEFAULT, useDefaultLogging = true)
        instance = KotlinEntityDataStore<Persistable>(configuration)
        val tables = SchemaModifier(configuration)
        val mode = TableCreationMode.DROP_CREATE
        tables.createTables(mode)

        // create dummy test data
        setupDummyData()
    }

    @After
    fun tearDown() {
        data.close()
    }

    fun setupDummyData() {
        // dummy Transaction
        val notary: Party = DUMMY_NOTARY
        val inState1 = TransactionState(DummyContract.SingleOwnerState(0, ALICE_PUBKEY), notary)
        val inState2 = TransactionState(DummyContract.MultiOwnerState(0,
                        listOf(ALICE_PUBKEY, BOB_PUBKEY)), notary)
        val inState3 = TransactionState(VaultNoopContract.VaultNoopState(CHARLIE_PUBKEY), notary)
        val outState1 = inState1.copy(notary = ALICE)
        val outState2 = inState2.copy(notary = BOB)
        val outState3 = inState3.copy(notary = CHARLIE)
        val inputs = listOf(StateAndRef(inState1, StateRef(SecureHash.randomSHA256(), 0)),
                            StateAndRef(inState2, StateRef(SecureHash.randomSHA256(), 0)),
                            StateAndRef(inState3, StateRef(SecureHash.randomSHA256(), 0)))
        val outputs = listOf(outState1, outState2, outState3)
        val commands = emptyList<AuthenticatedObject<CommandData>>()
        val attachments = emptyList<Attachment>()
        val id = SecureHash.randomSHA256()
        val signers = listOf(DUMMY_NOTARY_KEY.public.composite)
        val timestamp: Timestamp? = null
        transaction = LedgerTransaction(
                inputs,
                outputs,
                commands,
                attachments,
                id,
                notary,
                signers,
                timestamp,
                TransactionType.General()
        )
    }

    /**
     *  Vault Schema: VaultStates
     */
    @Test
    fun testInsertState() {
        val state = VaultStatesEntity()
        state.txId = "12345"
        state.index = 0
        data.invoke {
            insert(state)
            val result = select(VaultSchema.VaultStates::class) //where (VaultSchema.VaultStates::txId eq state.txId) limit 10
            Assert.assertSame(state, result().first())
        }
        data.invoke {
            val result = select(VaultSchema.VaultStates::class) //where (VaultSchema.VaultStates::txId eq state.txId) limit 10
            Assert.assertSame(state, result().first())
        }
    }

    @Test
    fun testUpsertUnconsumedState() {
        val stateEntity = createStateEntity(transaction!!.inputs[0])
        data.invoke {
            insert(stateEntity)
            val result = select(VaultSchema.VaultStates::class) //where (VaultSchema.VaultStates::txId eq state.txId) limit 10
            Assert.assertSame(stateEntity, result().first())
        }
    }

    @Test
    fun testUpsertConsumedState() {

        val stateEntity = createStateEntity(transaction!!.inputs[0])
        data.invoke {
            insert(stateEntity)
        }

        val keys = mapOf(   VaultStatesEntity.TX_ID to stateEntity.txId,
                            VaultStatesEntity.INDEX to stateEntity.index)
        val key = io.requery.proxy.CompositeKey(keys)

        data.invoke {
            val state = findByKey(VaultStatesEntity::class, key)
            assertNotNull(state)
            state?.let {
                state.stateStatus = VaultSchema.StateStatus.CONSENSUS_AGREED_CONSUMED
                state.consumed = Instant.now()
                update(state)

                val result = select(VaultSchema.VaultStates::class) //where (VaultSchema.VaultStates::txId eq state.txId) limit 10
                assertEquals(VaultSchema.StateStatus.CONSENSUS_AGREED_CONSUMED, result().first().stateStatus)
            }
        }
    }

    @Test
    fun testTransactionalUpsertState() {
//        data.withTransaction(TransactionIsolation.REPEATABLE_READ) {
        data.invoke {
            transaction!!.inputs.forEach {
                val stateEntity = createStateEntity(it)
                insert(stateEntity)
            }
            val result = select(VaultSchema.VaultStates::class) //where (VaultSchema.VaultStates::txId eq state.txId) limit 10
            Assert.assertSame(3, result().toList().size)
        }

        data.invoke {
            val result = select(VaultSchema.VaultStates::class) //where (VaultSchema.VaultStates::txId eq state.txId) limit 10
            Assert.assertSame(3, result().toList().size)
        }
    }

    private fun createStateEntity(stateAndRef: StateAndRef<*>): VaultStatesEntity {
        val stateRef = stateAndRef.ref
        val state = stateAndRef.state

        val stateEntity = VaultStatesEntity()
        stateEntity.txId = stateRef.txhash.toString()
        stateEntity.index = stateRef.index
        stateEntity.stateStatus = VaultSchema.StateStatus.CONSENSUS_AGREED_UNCONSUMED
        stateEntity.contractStateClassName = state.data.javaClass.toString()
        stateEntity.contractState = state.serialize().bytes
        stateEntity.notaryName = state.notary.name
        stateEntity.notarised = Instant.now()
        return stateEntity
    }

    /**
     *  Vault Schema: Transaction Notes
     */
    @Test
    fun testInsertTxnNote() {
        val txnNoteEntity = VaultTxnNoteEntity()
        txnNoteEntity.txId = "12345"
        txnNoteEntity.note = "Sample transaction note"
        data.invoke {
            insert(txnNoteEntity)
            val result = select(VaultSchema.VaultTxnNote::class) //where (VaultSchema.VaultStates::txId eq state.txId) limit 10
            Assert.assertSame(txnNoteEntity, result().first())
        }
    }

    @Test
    fun testFindTxnNote() {
        val txnNoteEntity = VaultTxnNoteEntity()
        txnNoteEntity.txId = "12345"
        txnNoteEntity.note = "Sample transaction note #1"
        val txnNoteEntity2 = VaultTxnNoteEntity()
        txnNoteEntity2.txId = "23456"
        txnNoteEntity2.note = "Sample transaction note #2"
        data.invoke {
            insert(txnNoteEntity)
            insert(txnNoteEntity2)
        }

        data.invoke {
            val result = select(VaultSchema.VaultTxnNote::class) where (VaultSchema.VaultTxnNote::txId eq txnNoteEntity2.txId)
            assertEquals(result().count(), 1)
            Assert.assertSame(txnNoteEntity2, result().first())
        }
    }

    /**
     *  Vault Schema: Cash Balances
     */
    @Test
    fun testInsertCashBalance() {
        val cashBalanceEntity = VaultCashBalancesEntity()
        cashBalanceEntity.currency = "GPB"
        cashBalanceEntity.amount = 12345
        data.invoke {
            insert(cashBalanceEntity)
            val result = select(VaultSchema.VaultCashBalances::class) //where (VaultSchema.VaultStates::txId eq state.txId) limit 10
            Assert.assertSame(cashBalanceEntity, result().first())
        }
    }

    @Test
    fun testUpdateCashBalance() {
        val cashBalanceEntity = VaultCashBalancesEntity()
        cashBalanceEntity.currency = "GPB"
        cashBalanceEntity.amount = 12345
        data.invoke {
            insert(cashBalanceEntity)
        }

        data.invoke {
            val state = findByKey(VaultCashBalancesEntity::class, cashBalanceEntity.currency)
            assertNotNull(state)
            state?.let {
                state.amount += 10000
                update(state)

                val result = select(VaultCashBalancesEntity::class) //where (VaultSchema.VaultStates::txId eq state.txId) limit 10
                assertEquals(22345, result().first().amount)
            }
        }
    }

    @Test
    fun testUpsertCashBalance() {
        val cashBalanceEntity = VaultCashBalancesEntity()
        cashBalanceEntity.currency = "GPB"
        cashBalanceEntity.amount = 12345

        data.invoke {
            val state = findByKey(VaultCashBalancesEntity::class, cashBalanceEntity.currency)
            state?.let {
                state.amount += 10000
            }
            val result = upsert(state ?: cashBalanceEntity)
            assertEquals(12345, result.amount)
        }
    }

    /**
     *  Vault Schema: VaultFungibleState
     */
//    @Test
//    fun testInsertFungibleState() {
//
//        val fstate = VaultFungibleStateEntity()
//        fstate.txId = "12345"
//        fstate.index = 0
//
//        val ownerKey = VaultKeyEntity()
//        ownerKey.txId = fstate.txId + 1
//        ownerKey.index = fstate.index!! + 1
//        ownerKey.key = "ownerKey"
//
//        val issuerKey = VaultKeyEntity()
//        issuerKey.txId = fstate.txId + 2
//        issuerKey.index = fstate.index!! + 2
//        issuerKey.key = "issuerKey"
//
//        fstate.ownerKey = ownerKey
//        fstate.issuerKey = issuerKey
//        fstate.participants = setOf()
//        fstate.exitKeys = setOf()
//
//        data.invoke {
//            insert(fstate)
//            val result = select(VaultSchema.VaultFungibleState::class) where (VaultSchema.VaultFungibleState::txId eq fstate.txId) limit 10
//            Assert.assertSame(fstate, result().first())
//        }
//    }
//
//    /**
//     *  Vault Schema: VaultLinearState
//     */
//    @Test
//    fun testInsertLinearState() {
//
//        val lstate = VaultLinearStateEntity()
//        lstate.txId = "12345"
//        lstate.index = 0
//        lstate.uuid = UUID.randomUUID()
//        data.invoke {
//            insert(lstate)
//            val result = select(VaultSchema.VaultFungibleState::class) where (VaultSchema.VaultFungibleState::txId eq lstate.txId) limit 10
//            Assert.assertSame(lstate, result().first())
//
//        }
//    }

//    @Test
//    fun testUnConsumedStates() {
//        val clazzName = "net.corda.core.contracts.DummyContract\$SingleOwnerState"
//        val stateAndRefs = unconsumedStates(Class.forName(clazzName) as Class<ContractState>)
//        assertNotNull(stateAndRefs)
//        assertTrue { stateAndRefs.size > 0 }
//        stateAndRefs.forEach {
//            println("ref  : ${it.ref}")
//            println("state: ${it.state}")
//        }
//    }
//
//    fun <T: ContractState> unconsumedStates(clazz: Class<T>): List<StateAndRef<T>> {
//        data.invoke {
//            transaction!!.inputs.forEach {
//                insert(createStateEntity(it))
//            }
//        }
//        val stateAndRefs =
//            data.invoke {
//                val result = select(VaultSchema.VaultStates::class)
//                        .where(VaultSchema.VaultStates::stateStatus eq VaultSchema.StateStatus.CONSENSUS_AGREED_UNCONSUMED)
////                            .and(Class.forName(VaultSchema.VaultStates::contractStateClassName).isInstance(T) eq true)
//                result.get().stream().map { it ->
//                    val stateRef = StateRef(SecureHash.parse(it.txId), it.index)
//                    val state = it.contractState.deserialize<TransactionState<T>>()
//                    StateAndRef(state, stateRef)
//                }.toList()
//            }
//        return stateAndRefs
//    }

    class VaultNoopContract() : Contract {
        override val legalContractReference = SecureHash.sha256("")
        data class VaultNoopState(override val owner: CompositeKey) : OwnableState {
            override val contract = VaultNoopContract()
            override val participants: List<CompositeKey>
                get() = listOf(owner)
            override fun withNewOwner(newOwner: CompositeKey) = Pair(Commands.Create(), copy(owner = newOwner))
        }
        interface Commands : CommandData {
            class Create : TypeOnlyCommandData(), Commands
        }

        override fun verify(tx: TransactionForContract) {
            // Always accepts.
        }
    }

    @Test
    fun testAllUnconsumedStates() {

        // setup date
        data.invoke {
            transaction!!.inputs.forEach {
                insert(createStateEntity(it))
            }
        }

        val clazzName = "net.corda.core.contracts.DummyContract\$SingleOwnerState"
        val clazz = Class.forName(clazzName) as Class<DummyContract.SingleOwnerState>
        println("classname: ${clazz.toString()}")
        val clazzType = clazz.typeName

        var stateAndRefs = unconsumedStatesR<ContractState>()
        stateAndRefs = unconsumedStates<ContractState>(ContractState::class.java)
        assertNotNull(stateAndRefs)
        assertTrue { stateAndRefs.size > 0 }
        stateAndRefs.forEach {
            println("ref  : ${it.ref}")
            println("state: ${it.state}")
        }
    }

    @Test
    fun tesUnconsumedDummyStates() {

        // setup date
        data.invoke {
            transaction!!.inputs.forEach {
                insert(createStateEntity(it))
            }
        }

        var stateAndRefs = unconsumedStatesR<DummyContract.State>()
        stateAndRefs = unconsumedStates<DummyContract.State>(DummyContract.State::class.java)
        assertNotNull(stateAndRefs)
        assertTrue { stateAndRefs.size == 2 }
        stateAndRefs.forEach {
            println("ref  : ${it.ref}")
            println("state: ${it.state}")
        }
    }

    @Test
    fun tesUnconsumedDummySingleOwnerStates() {

        // setup date
        data.invoke {
            transaction!!.inputs.forEach {
                insert(createStateEntity(it))
            }
        }

        var stateAndRefs = unconsumedStatesR<DummyContract.SingleOwnerState>()
        stateAndRefs = unconsumedStates<DummyContract.SingleOwnerState>(DummyContract.SingleOwnerState::class.java)
        assertNotNull(stateAndRefs)
        assertTrue { stateAndRefs.size == 1 }
        stateAndRefs.forEach {
            println("ref  : ${it.ref}")
            println("state: ${it.state}")
        }
    }

    inline fun <reified T: ContractState> unconsumedStatesR(): List<StateAndRef<T>> {
        val stateAndRefs =
            data.invoke {
                val result = select(VaultSchema.VaultStates::class)
                        .where(VaultSchema.VaultStates::stateStatus eq VaultSchema.StateStatus.CONSENSUS_AGREED_UNCONSUMED)
                //                        .and(VaultSchema.VaultStates::contractStateClassName eq T::class.toString())
                //                            .and(Class.forName(VaultSchema.VaultStates::contractStateClassName).isInstance(T) eq true)
                result.get()
                        .map { it ->
                            val stateRef = StateRef(SecureHash.parse(it.txId), it.index)
                            val state = it.contractState.deserialize<TransactionState<T>>()
                            StateAndRef(state, stateRef)
                        }.filter {
                    T::class.java.isAssignableFrom(it.state.data.javaClass)
                }.toList()
            }
        return stateAndRefs
    }

    fun <T: ContractState> unconsumedStates(clazz: Class<T>): List<StateAndRef<T>> {
        val stateAndRefs =
                data.invoke {
                    val result = select(VaultSchema.VaultStates::class)
                            .where(VaultSchema.VaultStates::stateStatus eq VaultSchema.StateStatus.CONSENSUS_AGREED_UNCONSUMED)
                    result.get()
                            .map { it ->
                                val stateRef = StateRef(SecureHash.parse(it.txId), it.index)
                                val state = it.contractState.deserialize<TransactionState<T>>()
                                StateAndRef(state, stateRef)
                            }
                            .filter {
                                clazz.isAssignableFrom(it.state.data.javaClass)
                            }.toList()
                }
        return stateAndRefs
    }

}