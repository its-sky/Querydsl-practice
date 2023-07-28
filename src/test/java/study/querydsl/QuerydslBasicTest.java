package study.querydsl;

import com.querydsl.core.Tuple;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.PersistenceUnit;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.QTeam;
import study.querydsl.entity.Team;

import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static study.querydsl.entity.QMember.*;
import static study.querydsl.entity.QTeam.*;

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

//	@Test
//	public void resultFetch(Pageable pageable) {
//		List<Member> fetch = queryFactory
//				.selectFrom(member)
//				.fetch();
//
//		Member fetchOne = queryFactory
//				.selectFrom(member)
//				.fetchOne();
//
//		Member fetchFirst = queryFactory
//				.selectFrom(member)
//				.fetchFirst();
//
//		List<Member> list = queryFactory
//				.selectFrom(member)
//				.offset(pageable.getOffset())
//				.limit(pageable.getPageSize())
//				.fetch(); // 페이징 처리, fetchResults, fetchCount가 deprecated 되어서 이렇게 페이징해야함
//
//	}

	/**
	 * 회원 정렬 순서
	 * 1. 회원 나이 내림차순(desc)
	 * 2. 회원 이름 올림차순(asc)
	 * 단 2에서 회원 이름이 없으면 마지막에 출력(null last)
	 */
	@Test
	public void sort() {
		em.persist(new Member(null, 100));
		em.persist(new Member("member5", 100));
		em.persist(new Member("member6", 100));

		List<Member> result = queryFactory
				.selectFrom(member)
				.where(member.age.eq(100))
				.orderBy(member.age.desc(),
						member.username.asc().nullsLast())
				.fetch();

		Member member5 = result.get(0);
		Member member6 = result.get(1);
		Member memberNull = result.get(2);

		assertEquals("member5", member5.getUsername());
		assertEquals("member6", member6.getUsername());
//		assertNull(memberNull.getUsername());
	}

	@Test
	public void paging1() {
		List<Member> result = queryFactory
				.selectFrom(member)
				.orderBy(member.username.desc())
				.offset(1)
				.limit(2)
				.fetch();

		assertEquals(2, result.size());
	}

	@Test
	public void paging2(Pageable pageable) {
		List<Member> list = queryFactory
				.selectFrom(member)
				.offset(pageable.getOffset())
				.limit(pageable.getPageSize())
				.fetch();

		assertEquals(4, list.size());
	}

	@Test
	public void aggregation() {
		List<Tuple> result = queryFactory
				.select(
						member.count(),
						member.age.sum(),
						member.age.avg(),
						member.age.min())
				.from(member)
				.fetch();

		Tuple tuple = result.get(0);
		assertEquals(4, tuple.get(member.count()));
		assertEquals(100, tuple.get(member.age.sum()));
		assertEquals(25, tuple.get(member.age.avg()));
		assertEquals(10, tuple.get(member.age.min()));
	}

	/**
	 * 팀의 이름과 각 팀의 평균 연령을 구해라.
	 * @throws Exception
	 */
	@Test
	public void group() throws Exception {
		List<Tuple> result = queryFactory
				.select(team.name, member.age.avg())
				.from(member)
				.join(member.team, team)
				.groupBy(team.name)
				.fetch();

		Tuple teamA = result.get(0);
		Tuple teamB = result.get(1);

		assertEquals("teamA", teamA.get(team.name));
		assertEquals(15, teamA.get(member.age.avg()));

		assertEquals("teamB", teamB.get(team.name));
		assertEquals(35, teamB.get(member.age.avg()));
	}

	/**
	 * 팀A에 소속된 모든 회원
	 */
	@Test
	public void join() {
		List<Member> result = queryFactory
				.selectFrom(member)
				.join(member.team, team)
				.where(team.name.eq("teamA"))
				.fetch();

		assertThat(result)
				.extracting("username")
				.containsExactly("member1", "member2");
	}

	/**
	 * 세타 조인
	 * 회원의 이름이 팀 이름과 같은 회원을 조회
	 * 억지 예시긴 함
	 */
	@Test
	public void theta_join() {
		em.persist(new Member("teamA"));
		em.persist(new Member("teamB"));

		List<Member> result = queryFactory
				.select(member)
				.from(member, team)
				.where(member.username.eq(team.name))
				.fetch();

		assertThat(result)
				.extracting("username")
				.containsExactly("teamA", "teamB");
	}

	/**
	 * 회원과 팀을 조인하면서, 팀 이름이 teamA인 팀만 조인, 회원은 모두 조회
	 * JPQL : select m, t from Member m left join m.team t on t.name = 'teamA'
	 */
	@Test
	public void join_on_filtering() {
		List<Tuple> result = queryFactory
				.select(member, team)
				.from(member)
				.leftJoin(member.team, team).on(team.name.eq("teamA"))
				.fetch();

		for (Tuple tuple : result) {
			System.out.println("tuple = " + tuple);
		}
	}

	/**
	 * 연관관계가 없는 엔티티를 외부 조인
	 * 회원 이름과 팀 이름이 같은 대상 외부 조인
	 */
	@Test
	public void join_on_no_relation() {
		em.persist(new Member("teamA"));
		em.persist(new Member("teamB"));
		em.persist(new Member("teamC"));

		List<Tuple> result = queryFactory
				.select(member, team)
				.from(member)
				.leftJoin(team).on(member.username.eq(team.name))
				.fetch();

//		assertThat(result)
//				.extracting("username")
//				.containsExactly("teamA", "teamB");

		for (Tuple tuple : result) {
			System.out.println("tuple = " + tuple);
		}
	}

	@PersistenceUnit
	EntityManagerFactory emf;

	@Test
	public void fetchJoinNo() throws Exception {
		em.flush();
		em.clear();

		Member findMember = queryFactory
				.selectFrom(member)
				.where(member.username.eq("member1"))
				.fetchOne();

		boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
		assertThat(loaded).as("페치 조인 미적용").isFalse();
	}

	@Test
	public void fetchJoinUse() throws Exception {
		em.flush();
		em.clear();

		Member findMember = queryFactory
				.selectFrom(member)
				.join(member.team, team).fetchJoin()
				.where(member.username.eq("member1"))
				.fetchOne();

		boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
		assertThat(loaded).as("페치 조인 적용").isTrue();
	}

	/**
	 * 나이가 가장 많은 회원 조회
	 */
	@Test
	public void subQuery() {

		QMember m = new QMember("memberSub");

		Member member1 = queryFactory
				.selectFrom(member)
				.where(member.age.eq(
						JPAExpressions
								.select(m.age.max())
								.from(m)
				)).fetchOne();

		assertThat(member1.getAge()).isEqualTo(40);
	}

	/**
	 * 나이가 평균 이상인 회원 조회
	 */
	@Test
	public void subQueryGoe() {

		QMember m = new QMember("memberSub");

		List<Member> result = queryFactory
				.selectFrom(member)
				.where(member.age.goe(
						JPAExpressions
								.select(m.age.avg())
								.from(m)
				)).fetch();

		assertThat(result).extracting("age")
				.containsExactly(30, 40);
	}

	/**
	 * 나이가 평균 이상인 회원 조회
	 */
	@Test
	public void subQueryIn() {

		QMember m = new QMember("memberSub");

		List<Member> result = queryFactory
				.selectFrom(member)
				.where(member.age.in(
						JPAExpressions
								.select(m.age)
								.from(m)
								.where(m.age.gt(10))
				)).fetch();

		assertThat(result).extracting("age")
				.containsExactly(20, 30, 40);
	}

	@Test
	public void subQueryInSelect() {
		QMember m = new QMember("memberSub");

		List<Tuple> result = queryFactory
				.select(member.username,
						JPAExpressions
								.select(m.age.avg())
								.from(m))
				.from(member)
				.fetch();

		for (Tuple tuple : result) {
			System.out.println("tuple = " + tuple);
		}
	}

	@Test
	public void caseWhenThen() throws Exception {
		List<String> result = queryFactory
				.select(member.age
						.when(10).then("열 살")
						.when(20).then("스무살")
						.otherwise("기타"))
				.from(member)
				.fetch();

		for (String s : result) {
			System.out.println("s = " + s);
		}
	}

	// DB에서 이렇게 데이터 조작하는건 왠만하면 지양하자, Application 혹은 Presentation Layer에서 조작하자
	@Test
	public void complexCase() throws Exception {
		List<String> result = queryFactory
				.select(new CaseBuilder()
						.when(member.age.between(0, 20)).then("0~20살")
						.when(member.age.between(21, 30)).then("21~30살")
						.otherwise("기타"))
				.from(member)
				.fetch();

		for (String s : result) {
			System.out.println("s = " + s);
		}
	}

	@Test
	public void constant() throws Exception {
		List<Tuple> result = queryFactory
				.select(member.username, Expressions.constant("A"))
				.from(member)
				.fetch();

		for (Tuple tuple : result) {
			System.out.println("tuple = " + tuple);
		}
	}

	// stringValue는 enum 처리할 때 굉장히 유용하다
	@Test
	public void concat() throws Exception {
		String s = queryFactory
				.select(member.username.concat("_").concat(member.age.stringValue()))
				.from(member)
				.where(member.username.eq("member1"))
				.fetchOne();

		System.out.println(s);
	}
}
