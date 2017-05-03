package mongo

import (
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"time"

	"github.com/marcusolsson/goddd/cargo"
	"github.com/marcusolsson/goddd/location"
	"github.com/marcusolsson/goddd/voyage"
)

type cargoRepository struct {
	db      string
	session *mgo.Session
}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	fmt.Printf("%s took %s\n", name, elapsed)
}

type timeCargoRepository struct {
	cargo *cargoRepository
}

func (r *timeCargoRepository) Remove(cargo *cargo.Cargo) error {
	defer timeTrack(time.Now(), "cargo Remove")
	return r.cargo.Remove(cargo)
}

func (r *cargoRepository) Remove(cargo *cargo.Cargo) error {
	sess := r.session.Copy()
	defer sess.Close()

	c := sess.DB(r.db).C("cargo")

	err := c.Remove(bson.M{"trackingid": cargo.TrackingID})

	return err
}

func (r *timeCargoRepository) Store(cargo *cargo.Cargo) error {
	defer timeTrack(time.Now(), "cargo Store")
	return r.cargo.Store(cargo)
}

func (r *cargoRepository) Store(cargo *cargo.Cargo) error {
	sess := r.session.Copy()
	defer sess.Close()

	c := sess.DB(r.db).C("cargo")

	_, err := c.Upsert(bson.M{"trackingid": cargo.TrackingID}, bson.M{"$set": cargo})

	return err
}

func (r *timeCargoRepository) Find(id cargo.TrackingID) (*cargo.Cargo, error) {
	defer timeTrack(time.Now(), "cargo Find")
	return r.cargo.Find(id)
}

func (r *cargoRepository) Find(id cargo.TrackingID) (*cargo.Cargo, error) {
	sess := r.session.Copy()
	defer sess.Close()

	c := sess.DB(r.db).C("cargo")

	var result cargo.Cargo
	if err := c.Find(bson.M{"trackingid": id}).One(&result); err != nil {
		if err == mgo.ErrNotFound {
			return nil, cargo.ErrUnknown
		}
		return nil, err
	}

	return &result, nil
}

func (r *timeCargoRepository) FindAll() []*cargo.Cargo {
	defer timeTrack(time.Now(), "cargo FindAll")
	return r.cargo.FindAll()
}

func (r *cargoRepository) FindAll() []*cargo.Cargo {
	sess := r.session.Copy()
	defer sess.Close()

	c := sess.DB(r.db).C("cargo")

	var result []*cargo.Cargo
	if err := c.Find(bson.M{}).All(&result); err != nil {
		return []*cargo.Cargo{}
	}

	return result
}

// NewCargoRepository returns a new instance of a MongoDB cargo repository.
func NewCargoRepository(db string, session *mgo.Session) (cargo.Repository, error) {
	r := &cargoRepository{
		db:      db,
		session: session,
	}

	index := mgo.Index{
		Key:        []string{"trackingid"},
		Unique:     true,
		DropDups:   true,
		Background: true,
		Sparse:     true,
	}

	sess := r.session.Copy()
	defer sess.Close()

	c := sess.DB(r.db).C("cargo")

	if err := c.EnsureIndex(index); err != nil {
		return nil, err
	}

	return &timeCargoRepository{
		cargo: r,
	}, nil
}

type locationRepository struct {
	db      string
	session *mgo.Session
}

type timeLocationRepository struct {
	location *locationRepository
}

func (r *timeLocationRepository) Find(locode location.UNLocode) (*location.Location, error) {
	defer timeTrack(time.Now(), "location Find")
	return r.location.Find(locode)
}

func (r *locationRepository) Find(locode location.UNLocode) (*location.Location, error) {
	sess := r.session.Copy()
	defer sess.Close()

	c := sess.DB(r.db).C("location")

	var result location.Location
	if err := c.Find(bson.M{"unlocode": locode}).One(&result); err != nil {
		if err == mgo.ErrNotFound {
			return nil, location.ErrUnknown
		}
		return nil, err
	}

	return &result, nil
}

func (r *timeLocationRepository) FindAll() []*location.Location {
	defer timeTrack(time.Now(), "location FindAll")
	return r.location.FindAll()
}

func (r *locationRepository) FindAll() []*location.Location {
	sess := r.session.Copy()
	defer sess.Close()

	c := sess.DB(r.db).C("location")

	var result []*location.Location
	if err := c.Find(bson.M{}).All(&result); err != nil {
		return []*location.Location{}
	}

	return result
}

func (r *locationRepository) store(l *location.Location) error {
	sess := r.session.Copy()
	defer sess.Close()

	c := sess.DB(r.db).C("location")

	_, err := c.Upsert(bson.M{"unlocode": l.UNLocode}, bson.M{"$set": l})

	return err
}

// NewLocationRepository returns a new instance of a MongoDB location repository.
func NewLocationRepository(db string, session *mgo.Session) (location.Repository, error) {
	r := &locationRepository{
		db:      db,
		session: session,
	}

	sess := r.session.Copy()
	defer sess.Close()

	c := sess.DB(r.db).C("location")

	index := mgo.Index{
		Key:        []string{"unlocode"},
		Unique:     true,
		DropDups:   true,
		Background: true,
		Sparse:     true,
	}

	if err := c.EnsureIndex(index); err != nil {
		return nil, err
	}

	initial := []*location.Location{
		location.Stockholm,
		location.Melbourne,
		location.Hongkong,
		location.Tokyo,
		location.Rotterdam,
		location.Hamburg,
	}

	for _, l := range initial {
		r.store(l)
	}

	return &timeLocationRepository{
		location: r,
	}, nil
}

type voyageRepository struct {
	db      string
	session *mgo.Session
}

type timeVoyageRepository struct {
	voyage *voyageRepository
}

func (r *timeVoyageRepository) Find(voyageNumber voyage.Number) (*voyage.Voyage, error) {
	defer timeTrack(time.Now(), "voyage Find")
	return r.voyage.Find(voyageNumber)
}

func (r *voyageRepository) Find(voyageNumber voyage.Number) (*voyage.Voyage, error) {
	sess := r.session.Copy()
	defer sess.Close()

	c := sess.DB(r.db).C("voyage")

	var result voyage.Voyage
	if err := c.Find(bson.M{"number": voyageNumber}).One(&result); err != nil {
		if err == mgo.ErrNotFound {
			return nil, voyage.ErrUnknown
		}
		return nil, err
	}

	return &result, nil
}

func (r *voyageRepository) store(v *voyage.Voyage) error {
	sess := r.session.Copy()
	defer sess.Close()

	c := sess.DB(r.db).C("voyage")

	_, err := c.Upsert(bson.M{"number": v.Number}, bson.M{"$set": v})

	return err
}

// NewVoyageRepository returns a new instance of a MongoDB voyage repository.
func NewVoyageRepository(db string, session *mgo.Session) (voyage.Repository, error) {
	r := &voyageRepository{
		db:      db,
		session: session,
	}

	sess := r.session.Copy()
	defer sess.Close()

	c := sess.DB(r.db).C("voyage")

	index := mgo.Index{
		Key:        []string{"number"},
		Unique:     true,
		DropDups:   true,
		Background: true,
		Sparse:     true,
	}

	if err := c.EnsureIndex(index); err != nil {
		return nil, err
	}

	initial := []*voyage.Voyage{
		voyage.V100,
		voyage.V300,
		voyage.V400,
		voyage.V0100S,
		voyage.V0200T,
		voyage.V0300A,
		voyage.V0301S,
		voyage.V0400S,
	}

	for _, v := range initial {
		r.store(v)
	}

	return &timeVoyageRepository{
		voyage: r,
	}, nil
}

type handlingEventRepository struct {
	db      string
	session *mgo.Session
}

type timeHandlingEventRepository struct {
	event *handlingEventRepository
}

func (r *timeHandlingEventRepository) Store(e cargo.HandlingEvent) {
	defer timeTrack(time.Now(), "event Store")
	r.event.Store(e)
}

func (r *handlingEventRepository) Store(e cargo.HandlingEvent) {
	sess := r.session.Copy()
	defer sess.Close()

	c := sess.DB(r.db).C("handling_event")

	_ = c.Insert(e)
}

func (r *timeHandlingEventRepository) QueryHandlingHistory(id cargo.TrackingID) cargo.HandlingHistory {
	defer timeTrack(time.Now(), "event QueryHandlingHistory")
	return r.event.QueryHandlingHistory(id)
}

func (r *handlingEventRepository) QueryHandlingHistory(id cargo.TrackingID) cargo.HandlingHistory {
	sess := r.session.Copy()
	defer sess.Close()

	c := sess.DB(r.db).C("handling_event")

	var result []cargo.HandlingEvent
	_ = c.Find(bson.M{"trackingid": id}).All(&result)

	return cargo.HandlingHistory{HandlingEvents: result}
}

// NewHandlingEventRepository returns a new instance of a MongoDB handling event repository.
func NewHandlingEventRepository(db string, session *mgo.Session) cargo.HandlingEventRepository {
	r := &handlingEventRepository{
		db:      db,
		session: session,
	}

	return &timeHandlingEventRepository{
		event: r,
	}
}
