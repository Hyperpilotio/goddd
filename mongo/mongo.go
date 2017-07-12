package mongo

import (
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"os"
	"time"

	"github.com/marcusolsson/goddd/cargo"
	"github.com/marcusolsson/goddd/location"
	"github.com/marcusolsson/goddd/voyage"
)

type Garbage struct {
	Garbage string
}

var GARBAGE_CARGO Garbage

func timed(start time.Time, method string) {
	elapsed := time.Since(start)
	fmt.Printf("%s took %s\n", method, elapsed)
}

type cargoRepository struct {
	db      string
	session *mgo.Session
}

func (r *cargoRepository) Remove(cargo *cargo.Cargo) error {
	start := time.Now()
	defer timed(start, "Removing a cargo")

	sess := r.session.Copy()
	defer sess.Close()

	c := sess.DB(r.db).C("cargo")

	err := c.Remove(bson.M{"trackingid": cargo.TrackingID})
	c.Remove(bson.M{"trackingid_g": cargo.TrackingID})

	return err
}

func (r *cargoRepository) Store(cargo *cargo.Cargo) error {
	start := time.Now()
	defer timed(start, "Storing a cargo")

	sess := r.session.Copy()
	defer sess.Close()

	c := sess.DB(r.db).C("cargo")

	_, err := c.Upsert(bson.M{"trackingid": cargo.TrackingID}, bson.M{"$set": cargo})

	c.Upsert(bson.M{"trackingid_g": cargo.TrackingID}, bson.M{"$set": GARBAGE_CARGO})

	return err
}

func (r *cargoRepository) Find(id cargo.TrackingID) (*cargo.Cargo, error) {
	start := time.Now()
	defer timed(start, "Finding a single cargo")

	sess := r.session.Copy()
	defer sess.Close()

	c := sess.DB(r.db).C("cargo")

	c.Find(bson.M{"trackingid_g": id}).One(&Garbage{})

	var result cargo.Cargo
	if err := c.Find(bson.M{"trackingid": id}).One(&result); err != nil {
		if err == mgo.ErrNotFound {
			return nil, cargo.ErrUnknown
		}
		return nil, err
	}

	return &result, nil
}

func (r *cargoRepository) FindAll() []*cargo.Cargo {
	sess := r.session.Copy()
	defer sess.Close()
	sess.SetBatch(300)

	c := sess.DB(r.db).C("cargo")

	var result []*cargo.Cargo
	start := time.Now()
	defer timed(start, "Find all cargos")
	if err := c.Find(bson.M{}).All(&result); err != nil {
		fmt.Println("Found error finding all cargos:" + err.Error())
		return []*cargo.Cargo{}
	}
	fmt.Printf("Found %d cargos\n", len(result))

	return result
}

// NewCargoRepository returns a new instance of a MongoDB cargo repository.
func NewCargoRepository(db string, session *mgo.Session) (cargo.Repository, error) {
	if os.Getenv("NO_PADDING") == "" {
		// Roughly 10kb
		for i := 0; i < 60*1024; i++ {
			GARBAGE_CARGO.Garbage += "a"
		}
	}

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

	return r, nil
}

type locationRepository struct {
	db      string
	session *mgo.Session
}

func (r *locationRepository) Find(locode location.UNLocode) (*location.Location, error) {
	start := time.Now()
	defer timed(start, "Find a location")

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

func (r *locationRepository) FindAll() []*location.Location {
	start := time.Now()
	defer timed(start, "Find all locations")

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
	start := time.Now()
	defer timed(start, "Saving a location")

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

	return r, nil
}

type voyageRepository struct {
	db      string
	session *mgo.Session
}

func (r *voyageRepository) Find(voyageNumber voyage.Number) (*voyage.Voyage, error) {
	start := time.Now()
	defer timed(start, "Find a voyage")

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
	start := time.Now()
	defer timed(start, "Storing a voyage")

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

	return r, nil
}

type handlingEventRepository struct {
	db      string
	session *mgo.Session
}

func (r *handlingEventRepository) Store(e cargo.HandlingEvent) {
	start := time.Now()
	defer timed(start, "Storing a handle event")

	sess := r.session.Copy()
	defer sess.Close()

	c := sess.DB(r.db).C("handling_event")

	_ = c.Insert(e)
}

func (r *handlingEventRepository) QueryHandlingHistory(id cargo.TrackingID) cargo.HandlingHistory {
	start := time.Now()
	defer timed(start, "Querying handle history for single cargo")

	sess := r.session.Copy()
	defer sess.Close()

	c := sess.DB(r.db).C("handling_event")

	var result []cargo.HandlingEvent
	_ = c.Find(bson.M{"trackingid": id}).All(&result)

	return cargo.HandlingHistory{HandlingEvents: result}
}

// NewHandlingEventRepository returns a new instance of a MongoDB handling event repository.
func NewHandlingEventRepository(db string, session *mgo.Session) cargo.HandlingEventRepository {
	return &handlingEventRepository{
		db:      db,
		session: session,
	}
}
