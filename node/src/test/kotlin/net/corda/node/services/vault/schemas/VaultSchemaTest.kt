package net.corda.node.services.vault.schemas

import io.requery.Persistable
import io.requery.TransactionIsolation
import io.requery.kotlin.invoke
import io.requery.sql.KotlinConfiguration
import io.requery.sql.KotlinEntityDataStore
import io.requery.sql.SchemaModifier
import io.requery.sql.TableCreationMode
import junit.framework.Assert.assertEquals
import net.corda.core.contracts.*
import net.corda.core.crypto.Party
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.composite
import net.corda.core.transactions.LedgerTransaction
import net.corda.core.utilities.DUMMY_NOTARY
import net.corda.core.utilities.DUMMY_NOTARY_KEY
import net.corda.testing.ALICE
import net.corda.testing.ALICE_PUBKEY
import net.corda.testing.BOB
import net.corda.testing.BOB_PUBKEY
import org.h2.jdbcx.JdbcDataSource
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import java.time.Instant
import kotlin.test.assertNotNull

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
        val inState2 = TransactionState(DummyContract.SingleOwnerState(0, BOB_PUBKEY), notary)
        val outState1 = inState1.copy(notary = ALICE)
        val outState2 = inState1.copy(notary = BOB)
        val inputs = listOf(StateAndRef(inState1, StateRef(SecureHash.randomSHA256(), 0)),
                            StateAndRef(inState2, StateRef(SecureHash.randomSHA256(), 0)))
        val outputs = listOf(outState1, outState2)
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
        data.withTransaction(TransactionIsolation.REPEATABLE_READ) {
            transaction!!.inputs.forEach {
                val stateEntity = createStateEntity(it)
                insert(stateEntity)
            }
            val result = select(VaultSchema.VaultStates::class) //where (VaultSchema.VaultStates::txId eq state.txId) limit 10
            Assert.assertSame(2, result().toList().size)
        }

//        data.invoke {
//            val result = select(VaultSchema.VaultStates::class) //where (VaultSchema.VaultStates::txId eq state.txId) limit 10
//            Assert.assertSame(2, result().toList().size)
//        }
    }

    private fun createStateEntity(stateAndRef: StateAndRef<*>): VaultStatesEntity {
        val stateRef = stateAndRef.ref
        val state = stateAndRef.state

        val stateEntity = VaultStatesEntity()
        stateEntity.txId = stateRef.txhash.toString()
        stateEntity.index = stateRef.index
        stateEntity.stateStatus = VaultSchema.StateStatus.CONSENSUS_AGREED_UNCONSUMED
        stateEntity.contractStateClassName = state.data.javaClass.toString()
        //state.contractStateClassVersion = it.value.state.data.version
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
        txnNoteEntity.note = "Sample transaction note"
        data.invoke {
            insert(txnNoteEntity)
        }

        data.invoke {
            val result = select(VaultSchema.VaultTxnNote::class) //where (VaultSchema.VaultTxnNote::txId eq txnNoteEntity.txId)
            Assert.assertSame(txnNoteEntity, result().first())
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
}