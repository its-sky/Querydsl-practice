package study.querydsl;

import com.querydsl.jpa.impl.JPAQueryFactory;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.Team;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static study.querydsl.entity.QMember.*;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

	@PersistenceContext
	EntityManager em;

	JPAQueryFactory queryFactory;

	@BeforeEach
	public void before() {
		queryFactory = new JPAQueryFactory(em);
		Team teamA = new Team("teamA");
		Team teamB = new Team("teamB");
		em.persist(teamA);
		em.persist(teamB);

		Member member1 = new Member("member1", 10, teamA);
		Member member2 = new Member("member2", 20, teamA);

		Member member3 = new Member("member3", 30, teamB);
		Member member4 = new Member("member4", 40, teamB);
		em.persist(member1);
		em.persist(member2);
		em.persist(member3);
		em.persist(member4);

		// 초기화
		em.flush();
		em.clear();

		List<Member> members = em.createQuery("select m from Member m", Member.class).getResultList();

		for (Member member : members) {
			System.out.println("member = " + member);
			System.out.println("-> member.getTeam" + member.getTeam());
		}
	}

	@Test
	public void startJPQL() {
		//member1을 찾아라
		String qlString = "select m from Member m " +
				"where m.username = :username";
		Member findMember = em.createQuery(qlString, Member.class)
				.setParameter("username", "member1")
				.getSingleResult();

		assertEquals(findMember.getUsername(), "member1");
	}

	@Test
	public void startQuertdsl() {
		Member findOne = queryFactory
				.select(member)
				.from(member)
				.where(member.username.eq("member1"))
				.fetchOne();

		assertEquals(findOne.getUsername(), "member1");
	}

	@Test
	public void search() {
		Member findOne = queryFactory
				.selectFrom(member)
				.where(member.username.eq("member1").and(member.age.eq(10)))
				.fetchOne();

		assertEquals("member1", findOne.getUsername());
	}

	@Test
	public void searchAndParam() {
		Member findOne = queryFactory
				.selectFrom(member)
				.where(
						member.username.eq("member1"),
						member.age.eq(10)
				)
				.fetchOne();

		assertEquals("member1", findOne.getUsername());
	}
}
