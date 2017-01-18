package net.corda.node.services.vault.schemas

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.requery.Persistable
import io.requery.kotlin.eq
import io.requery.kotlin.invoke
import io.requery.sql.KotlinConfiguration
import io.requery.sql.KotlinEntityDataStore
import io.requery.sql.SchemaModifier
import io.requery.sql.TableCreationMode
import net.corda.testing.node.makeTestDataSourceProperties
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test

class VaultSchemaTest {

    var instance : KotlinEntityDataStore<Persistable>? = null
    val data : KotlinEntityDataStore<Persistable> get() = instance!!

    @Before
    fun setUp() {
        val config = HikariConfig(makeTestDataSourceProperties())
        val dataSource = HikariDataSource(config)
        val configuration = KotlinConfiguration(dataSource = dataSource, model = Models.DEFAULT, useDefaultLogging = true)
        instance = KotlinEntityDataStore<Persistable>(configuration)
        val tables = SchemaModifier(configuration)
        val mode = TableCreationMode.CREATE
        tables.createTables(mode)
    }

    @After
    fun tearDown() {
        data.close()
    }

    /**
     *  Vault Schema: VaultStates
     */
    @Test
    fun testInsertState() {
        val state = VaultStatesEntity()
        state.txId ="12345"
        state.index = 0
        data.invoke {
            insert(state)
            val result = select(VaultSchema.VaultStates::class) where (VaultSchema.VaultStates::txId eq state.txId) limit 10
            Assert.assertSame(result().first(), state)

        }
    }

    /**
     *  Vault Schema: VaultFungibleState
     */
    companion object {
        fun randomFungibleState(): VaultFungibleState {
            val random = Random()
            val state = VaultFungibleStateEntity()
            state.txId = "12345"
            state.index = 0
            state.quantity = random.nextLong()
            val ccyCodes = arrayOf("GBP","USD","CHF","EUR")
            state.ccyCode = ccyCodes[random.nextInt(ccyCodes.size)]
            state.participants = VaultKeyEntity()
            state.issuerKey = VaultKeyEntity()
            state.issuerRef = byteArrayOf(1)
            state.exitKeys = VaultKeyEntity()
            return state
        }
    }


    @Test
    fun `insert into fungible state`() {
        val fungibleState = randomFungibleState()
        data.invoke {
            insert(fungibleState)
            val result = select(VaultFungibleState::class) where (VaultFungibleState::txId eq fungibleState.txId) limit 10
            Assert.assertSame(result().first(), fungibleState)
        }
    }

    @Test
    fun `query first from fungible state`() {
        data.invoke {
            val result = select(VaultFungibleState::class) where (VaultFungibleState::ccyCode eq "GBP") limit 5
            val first = result.get().first()
            println(first)
            assertNotNull(first)
        }
    }
}